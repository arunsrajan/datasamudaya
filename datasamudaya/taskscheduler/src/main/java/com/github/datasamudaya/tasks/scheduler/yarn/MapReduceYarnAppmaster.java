/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.tasks.scheduler.yarn;

import java.net.InetAddress;
import java.net.URI;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jooq.lambda.tuple.Tuple2;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.TaskInfoYARN;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.google.common.collect.Iterables;

/**
 * 
 * @author Arun
 * Yarn App master with lifecycle init, submitapplication, isjobcomplete and prelaunch containers.
 * Various container events captured are container failure and completed operation 
 * with container statuses.
 */
public class MapReduceYarnAppmaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(MapReduceYarnAppmaster.class);

	private final Map<String, Object> pendingjobs = new ConcurrentHashMap<>();
	private final Semaphore lock = new Semaphore(1);
	@SuppressWarnings("rawtypes")
	private DataCruncherContext dcc;
	private long taskcompleted, redtaskcompleted;
	private int tasksubmitted;
	private int redtasksubmitted;
	private int numreducers;
	TaskInfoYARN tinfo = new TaskInfoYARN();
	SimpleDistributedQueue outputqueue;

	private long taskidcounter;
	private boolean tokillcontainers;
	private boolean isreadytoexecute;
	private String applicationid = "";

	/**
	 * Container initialization.
	 */
	@Override
	protected void onInit() throws Exception {
		StaticComponentContainer.Modules.exportAllToAll();
		super.onInit();
		if (getLauncher() instanceof AbstractLauncher launcher) {
			launcher.addInterceptor(this);
		}
	}
	Map<String, Set<Object>> mapclzchunkfile;
	Set<Object> combiner;
	Set<Object> reducer;
	Map<String, List<BlocksLocation>> folderfileblocksmap;
	List<YarnReducer> rs = new ArrayList<>();
	JobConfiguration jobconf;
	Map<String, List<MapperCombiner>> ipmcs = new ConcurrentHashMap<>();
	int totalmappersize;
	Map<String, String> containeridipmap = new ConcurrentHashMap<>();
	Map<String, Integer> iptasksubmittedmap = new ConcurrentHashMap<>();

	/**
	 * Submit the user application. The various parameters obtained from
	 * HDFS are graph with node and edges, job stage map, job stage with 
	 * task information.
	 */
	@SuppressWarnings("unchecked")
	@Override
	public void submitApplication() {

		ExecutorService es = null;
		try {
			var appmasterservice = (MapReduceYarnAppmasterService) getAppmasterService();
			log.debug("In SubmitApplication Setting AppMaster Service: " + appmasterservice);
			if (appmasterservice != null) {
				// Set the Yarn App master bean to the Yarn App master service object.
				appmasterservice.setYarnAppMaster(this);
			}
			es = Executors.newFixedThreadPool(1);
			es.execute(() -> pollQueue());
			var prop = new Properties();
			DataSamudayaProperties.put(prop);
			prop.put(DataSamudayaConstants.HDFSNAMENODEURL, getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL));
			ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
			var containerallocator = (DefaultContainerAllocator) getAllocator();
			log.debug("Parameters: " + getParameters());
			log.debug("Container-Memory: " + getParameters().getProperty("container-memory", "1024"));
			log.debug("Container-Cpu: " + getParameters().getProperty("container-cpu", "1"));
			containerallocator.setVirtualcores(Integer.parseInt(getParameters().getProperty("container-cpu", "1")));
			containerallocator.setMemory(Integer.parseInt(getParameters().getProperty("container-memory", "1024")));
		} catch (Exception ex) {
			log.debug("Submit Application Error, See cause below \n", ex);
		}
		super.submitApplication();
	}

	@SuppressWarnings("unchecked")
	protected void pollQueue() {
		log.debug("Task Id Counter: " + taskidcounter);
		log.debug("Environment: " + getEnvironment());
		try (var zo = new ZookeeperOperations();) {
			zo.connect();
			String teid = getEnvironment().get(DataSamudayaConstants.YARNDATASAMUDAYAJOBID);
			SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_INPUT_QUEUE
					+ DataSamudayaConstants.FORWARD_SLASH + teid);

			outputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_OUTPUT_QUEUE
					+ DataSamudayaConstants.FORWARD_SLASH + teid);

			ObjectMapper objectMapper = new ObjectMapper();
			var prop = new Properties();
			DataSamudayaProperties.put(prop);
			while (!tokillcontainers) {
				if (inputqueue.peek() != null && !isreadytoexecute) {
					pendingjobs.clear();
					tinfo = objectMapper.readValue(inputqueue.poll(), TaskInfoYARN.class);
					tokillcontainers = tinfo.isTokillcontainer();
					if (Objects.isNull(tinfo.getJobid())) {
						continue;
					}
					var yarninputfolder = DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH
							+ tinfo.getJobid();
					log.debug("Yarn Input Folder: " + yarninputfolder);
					log.debug("AppMaster HDFS: " + getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL));
					System.setProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL));
					var configuration = getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL);
					// Thread containing the job stage information.
					mapclzchunkfile = (Map<String, Set<Object>>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(
							configuration, yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_MAPPER);
					combiner = (Set<Object>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(configuration,
							yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_COMBINER);
					reducer = (Set<Object>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(configuration,
							yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_REDUCER);
					folderfileblocksmap = (Map<String, List<BlocksLocation>>) RemoteDataFetcher
							.readYarnAppmasterServiceDataFromDFS(configuration, yarninputfolder,
									DataSamudayaConstants.MASSIVEDATA_YARNINPUT_FILEBLOCKS);
					jobconf = (JobConfiguration) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(configuration,
							yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_CONFIGURATION);
					numreducers = Integer.parseInt(jobconf.getNumofreducers());

					var taskcount = 0;
					dcc = new DataCruncherContext();
					applicationid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
					var bls = folderfileblocksmap.keySet().stream().flatMap(key -> folderfileblocksmap.get(key).stream())
							.collect(Collectors.toList());
					List<MapperCombiner> mappercombiners;
					totalmappersize = bls.size();
					for (var bl : bls) {
						var taskid = DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + DataSamudayaConstants.MAPPER + DataSamudayaConstants.HYPHEN
								+ (taskcount + 1);
						var apptask = new ApplicationTask();
						apptask.setApplicationid(applicationid);
						apptask.setTaskid(taskid);
						var mc = (MapperCombiner) getMapperCombiner(mapclzchunkfile, combiner, bl, apptask);
						var xrefdnaddrs = new ArrayList<>(bl.getBlock()[0].getDnxref().keySet());
						var key = xrefdnaddrs.get(taskcount % xrefdnaddrs.size());
						var dnxrefaddr = new ArrayList<>(bl.getBlock()[0].getDnxref().get(key));
						bl.getBlock()[0].setHp(dnxrefaddr.get(taskcount % dnxrefaddr.size()));
						var host = bl.getBlock()[0].getHp().split(":")[0];
						if (Objects.isNull(ipmcs.get(host))) {
							mappercombiners = new ArrayList<>();
							ipmcs.put(host, mappercombiners);
						} else {
							mappercombiners = ipmcs.get(host);
						}
						mappercombiners.add(mc);
						taskcount++;
					}
					log.debug(bls);
					taskcount = 0;
					while (taskcount < numreducers) {
						var red = new YarnReducer();
						red.reducerclasses = reducer;
						var taskid = DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + DataSamudayaConstants.REDUCER + DataSamudayaConstants.HYPHEN
								+ (taskcount + 1);
						var apptask = new ApplicationTask();
						apptask.setApplicationid(applicationid);
						apptask.setTaskid(taskid);
						red.apptask = apptask;
						rs.add(red);
						taskcount++;
					}
					log.debug(rs);
					isreadytoexecute = true;
					log.debug("tasks To Execute:" + tinfo.getJobid());
				} else {
					Thread.sleep(1000);
				}
			}
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	public MapperCombiner getMapperCombiner(
			Map<String, Set<Object>> mapclzchunkfile,
			Set<Object> combiners, BlocksLocation blockslocation, ApplicationTask apptask) {
		var rawpath = blockslocation.getBlock()[0].getFilename().split(DataSamudayaConstants.FORWARD_SLASH);
		var mapcombiner = new MapperCombiner(blockslocation, mapclzchunkfile.get(DataSamudayaConstants.FORWARD_SLASH + rawpath[3]), apptask,
				combiners);
		return mapcombiner;
	}

	/**
	 * Set App Master service hosts and port running before the container is launched.
	 */
	@Override
	public ContainerLaunchContext preLaunch(Container container, ContainerLaunchContext context) {
		var service = getAppmasterService();
		if (service != null) {
			Map<String, String> env = null;
			try {
				containeridipmap.put(container.getId().toString().trim(), container.getNodeId().getHost());
				var port = service.getPort();
				var address = InetAddress.getLocalHost().getHostAddress();
				log.debug("preLaunch Container Id Ip Map:" + containeridipmap);
				log.debug("App Master Service Ip Address: " + address);
				log.debug("App Master Service Port: " + port);
				log.debug("Container Id: " + container.getId().toString() + " Ip: " + service.getHost());
				env = new HashMap<>(context.getEnvironment());
				//Set the service port to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));

				//Set the service host to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_HOST, address);
			} catch (Exception ex) {
				log.debug("Container Prelaunch error, See cause below \n", ex);
			}
			context.setEnvironment(env);
			return context;
		} else {
			return context;
		}
	}

	/**
	 * Execute the OnContainer completed method when container is exited with
	 * the exitcode.
	 */
	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		try {
			lock.acquire();
			super.onContainerCompleted(status);
			log.debug("Container completed: " + status.getContainerId());
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.debug("Container Completion fails", ex);
		}
		finally {
			lock.release();
		}
	}

	/**
	 * Execute the OnContainer failed method when container is exited with
	 * the exitcode.
	 */
	@Override
	protected boolean onContainerFailed(ContainerStatus status) {
		try {
			lock.acquire();
			log.debug("Container failed: " + status.getContainerId());
			return true;
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return false;
		} catch (Exception ex) {
			log.debug("Container allocation fails", ex);
			return false;
		}
		finally {
			lock.release();
		}
	}


	/**
	 * Update the job statuses if job status is completed.
	 *
	 * @param mc
	 * @param success
	 * @param containerid
	 */
	@SuppressWarnings("unchecked")
	public void reportJobStatus(MapperCombiner mc, boolean success, String containerid) {
		try {
			lock.acquire();
			if (success) {
				log.debug(mc.apptask.getApplicationid() + mc.apptask.getTaskid() + " Updated");
				var keys = (Set<Object>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(mc.apptask.getApplicationid(),
						(mc.apptask.getApplicationid() + mc.apptask.getTaskid()), true);
				dcc.putAll(keys, mc.apptask.getApplicationid() + mc.apptask.getTaskid());
				log.debug("dcc: " + dcc);
				pendingjobs.remove(mc.apptask.getApplicationid() + mc.apptask.getTaskid());
				taskcompleted++;
			} else {
				pendingjobs.put(mc.apptask.getApplicationid() + mc.apptask.getTaskid(), mc);
			}
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.debug("reportJobStatus fails", ex);
		}
		finally {
			lock.release();
		}
	}


	public void reportJobStatus(YarnReducer r, boolean success, String containerid) {
		try {
			lock.acquire();
			if (success) {
				RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(rs, DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH + applicationid,
						DataSamudayaConstants.MASSIVEDATA_YARN_RESULT, jobconf);
				if (redtaskcompleted + 1 == rs.size()) {
					var sb = new StringBuilder();
					for (var redcount = 0;redcount < numreducers;redcount++) {
						var red = rs.get(redcount);
						var ctxreducerpart = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(
								red.apptask.getApplicationid(),
								(red.apptask.getApplicationid() + red.apptask.getTaskid()), false);
						var keysreducers = ctxreducerpart.keys();
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append("Partition ").append(redcount + 1).append("-------------------------------------------------");
						sb.append(DataSamudayaConstants.NEWLINE);
						for (var key : keysreducers) {
							sb.append(key + DataSamudayaConstants.SINGLESPACE + ctxreducerpart.get(key));
							sb.append(DataSamudayaConstants.NEWLINE);
						}
						sb.append("-------------------------------------------------");
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append(DataSamudayaConstants.NEWLINE);
					}
					var filename = DataSamudayaConstants.MAPRED + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
					log.debug("Writing Results to file: " + filename);
					try (var hdfs = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)),
							new Configuration());var fsdos = hdfs.create(new Path(
							DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
									+ jobconf.getOutputfolder() + DataSamudayaConstants.FORWARD_SLASH + filename));) {
						fsdos.write(sb.toString().getBytes());
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					ObjectMapper objMapper = new ObjectMapper();
					tinfo.setIsresultavailable(true);
					tinfo.setJobid(applicationid);
					outputqueue.offer(objMapper.writeValueAsBytes(tinfo));
					tasksubmitted = 0;
					redtasksubmitted = 0;
					taskcompleted = 0;
					redtaskcompleted = 0;
					rs.clear();
					pendingjobs.remove(r.apptask.getApplicationid() + r.apptask.getTaskid());
					isreadytoexecute = false;
					return;
				}
				log.debug(r.apptask.getApplicationid() + r.apptask.getTaskid() + " Updated");
				pendingjobs.remove(r.apptask.getApplicationid() + r.apptask.getTaskid());
				redtaskcompleted++;
			} else {
				pendingjobs.put(r.apptask.getApplicationid() + r.apptask.getTaskid(), r);
			}
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		}
		catch (Exception ex) {
			log.debug("reportJobStatus fails", ex);
		}
		finally {
			lock.release();
		}
	}

	Iterator<List<Tuple2>> partkeys;

	/**
	 * Obtain the job to execute
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object getJob(String containerid) {
		try {
			lock.acquire();
			if (isreadytoexecute) {
				if (!pendingjobs.keySet().isEmpty()) {
					return pendingjobs.remove(pendingjobs.keySet().iterator().next());
				} else if (tasksubmitted < totalmappersize) {
					log.debug("getJob Container Id Ip Map:" + containeridipmap);
					var ip = containeridipmap.get(containerid.trim());
					var iptasksubmitted = iptasksubmittedmap.get(ip) == null ? 0 : iptasksubmittedmap.get(ip);
					var mcs = ipmcs.get(ip);
					if (Objects.isNull(mcs)) {
						return null;
					}
					if (iptasksubmitted < mcs.size()) {
						var mc = ipmcs.get(ip).get(iptasksubmitted++);
						iptasksubmittedmap.put(ip, iptasksubmitted);
						tasksubmitted++;
						return mc;
					}
				} else if (redtasksubmitted < rs.size() && taskcompleted >= totalmappersize) {
					if (redtasksubmitted == 0) {
						var keyapptasks = (List<Tuple2>) dcc.keys().parallelStream()
								.map(key -> new Tuple2(key, dcc.get(key)))
								.collect(Collectors.toCollection(ArrayList::new));
						partkeys = Iterables.partition(keyapptasks, (keyapptasks.size()) / numreducers).iterator();
					}
					rs.get(redtasksubmitted).tuples = new ArrayList<Tuple2>(partkeys.next());
					log.debug("Tuples: " + rs.get(redtasksubmitted).tuples);
					return rs.get(redtasksubmitted++);
				}
			}
			return null;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return null;
		} catch (Exception ex) {
			log.debug("reportJobStatus fails", ex);
			return null;
		} finally {
			lock.release();
		}
	}

	/**
	 * Check on whether the jobs are available to execute.
	 * @return
	 */
	public boolean hasJobs() {
		try {
			lock.acquire();
			boolean hasJobs = isreadytoexecute && (pendingjobs.size() > 0 || taskcompleted < totalmappersize || redtaskcompleted < rs.size());
			return hasJobs || !tokillcontainers;
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return false;
		} catch (Exception ex) {
			log.debug("hasJobs fails", ex);
			return false;
		}
		finally {
			lock.release();
		}
	}


	@Override
	public String toString() {
		return DataSamudayaConstants.PENDINGJOBS + DataSamudayaConstants.EQUAL + pendingjobs.size() + DataSamudayaConstants.SINGLESPACE + DataSamudayaConstants.RUNNINGJOBS + DataSamudayaConstants.EQUAL + pendingjobs.size();
	}
}
