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
package com.github.datasamudaya.stream.yarn.appmaster;

import static java.util.Objects.nonNull;

import java.net.InetAddress;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.Timer;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerState;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.apache.hadoop.yarn.api.records.ContainerSubState;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.esotericsoftware.kryo.Kryo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.EXECUTORTYPE;
import com.github.datasamudaya.common.GetTaskActor;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskExecutorShutdown;
import com.github.datasamudaya.common.TaskInfoYARN;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.scheduler.RemoteJobScheduler;
import com.github.datasamudaya.stream.scheduler.StreamPipelineTaskSubmitter;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;
import com.github.dexecutor.core.task.TaskProvider;

/**
 * 
 * @author Arun 
 * Yarn App master with lifecycle init, submitapplication,
 * isjobcomplete and prelaunch containers. Various container events
 * captured are container failure and completed operation with container
 * statuses.
 */
public class StreamPipelineSQLYarnAppmaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private static final Logger log = LoggerFactory.getLogger(StreamPipelineSQLYarnAppmaster.class);
	private List<Task> tasks = new Vector<>();

	private final Map<String, Task> pendingtasks = new ConcurrentHashMap<>();
	private final Map<String, Task> pendingsubmittedtasks = new ConcurrentHashMap<>();
	private final Map<String, Map<String, Task>> containertaskmap = new ConcurrentHashMap<>();
	private Map<String, JobStage> jsidjsmap;
	private String teid;
	private final Map<String, Boolean> sentjobstages = new ConcurrentHashMap<>();
	TaskInfoYARN tinfo = new TaskInfoYARN();
	private final Object lock = new Object();
	SimpleDistributedQueue outputqueue;
	private PipelineConfig pipelineconfig = null;
	private long taskcompleted;
	private boolean tokillcontainers;
	private boolean isreadytoexecute;
	private List<StreamPipelineTaskSubmitter> mdststs;
	private SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
			DAGEdge.class);
	private ZookeeperOperations zoglobal;
	private Map<String, StreamPipelineTaskSubmitter> taskmdsthread;
	List<String> tes;
	boolean isdriverallocated = false;
	boolean isdriverrequired = false;
	Semaphore executoralloclock = new Semaphore(1);
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

	/**
	 * Submit the user application. The various parameters obtained from HDFS are
	 * graph with node and edges, job stage map, job stage with task information.
	 */
	@Override
	public void submitApplication() {
		ExecutorService es = null;
		try {
			es = Executors.newFixedThreadPool(1, Thread.ofVirtual().factory());
			teid = getEnvironment().get(DataSamudayaConstants.YARNDATASAMUDAYAJOBID);
			isdriverrequired = Boolean.parseBoolean(getEnvironment().get(DataSamudayaConstants.ISDRIVERREQUIREDYARN));
			es.execute(() -> pollQueue());
			var prop = new Properties();
			DataSamudayaProperties.put(prop);
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
		var appmasterservice = (StreamPipelineSQLYarnAppmasterService) getAppmasterService();
		log.debug("In SubmitApplication Setting AppMaster Service: " + appmasterservice);
		if (appmasterservice != null) {
			// Set the Yarn App master bean to the Yarn App master service object.
			appmasterservice.setSQLYarnAppMaster(this);
		}		
		super.submitApplication();
	}
	Map<String, String> containeridipmap = new ConcurrentHashMap<>();

	@SuppressWarnings("unchecked")
	protected void pollQueue() {
		log.debug("Environment: " + getEnvironment());
		Job job;
		try (var zo = new ZookeeperOperations();) {
			zo.connect();		
			zoglobal = zo;
			SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_INPUT_QUEUE
					+ DataSamudayaConstants.FORWARD_SLASH + teid);

			outputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.YARN_OUTPUT_QUEUE
					+ DataSamudayaConstants.FORWARD_SLASH + teid);			
			ObjectMapper objectMapper = new ObjectMapper();
			while (!tokillcontainers) {
				if (inputqueue.peek() != null && !isreadytoexecute) {
					pendingtasks.clear();
					pendingsubmittedtasks.clear();
					containertaskmap.clear();
					sentjobstages.clear();
					taskcompleted = 0;
					tinfo = objectMapper.readValue(inputqueue.poll(), TaskInfoYARN.class);
					tokillcontainers = tinfo.isTokillcontainer();
					if (Objects.isNull(tinfo.getJobid())) {
						continue;
					}
					var yarninputfolder = DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH
							+ tinfo.getJobid();
					log.debug("Yarn Input Folder: " + yarninputfolder);
					log.debug("AppMaster HDFS: " + getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL));
					var namenodeurl = getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL);
					System.setProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							getConfiguration().get(DataSamudayaConstants.HDFSNAMENODEURL));
					// Thread containing the job stage information.
					mdststs = (List<StreamPipelineTaskSubmitter>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(
							namenodeurl, yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_DATAFILE);
					// Graph containing the nodes and edges.
					graph = (SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>) RemoteDataFetcher
							.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
									DataSamudayaConstants.MASSIVEDATA_YARNINPUT_GRAPH_FILE);
					// task map.
					taskmdsthread = (Map<String, StreamPipelineTaskSubmitter>) RemoteDataFetcher
							.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
									DataSamudayaConstants.MASSIVEDATA_YARNINPUT_TASK_FILE);
					jsidjsmap = (Map<String, JobStage>) RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS(namenodeurl,
							yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE);
					
					pipelineconfig = (PipelineConfig) RemoteDataFetcher
							.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
									DataSamudayaConstants.MASSIVEDATA_YARNINPUT_CONFIGFILE);
					job = (Job) RemoteDataFetcher
							.readYarnAppmasterServiceDataFromDFS(namenodeurl, yarninputfolder,
									DataSamudayaConstants.MASSIVEDATA_YARNINPUT_JOB_FILE);
					
					tasks = graph.vertexSet().stream().map(StreamPipelineTaskSubmitter::getTask).collect(Collectors.toCollection(Vector::new));
					if(isdriverallocated) {
						RemoteJobScheduler rjs = new RemoteJobScheduler();
						job.setVertices(new LinkedHashSet<>(graph.vertexSet()));
						job.setEdges(new LinkedHashSet<>(graph.edgeSet()));
						job.setJsidjsmap(jsidjsmap);
						rjs.scheduleJob(job);
					}
					else {
						broadcastJobStageToTaskExecutors(tasks);
						parallelExecutionAkkaActors(graph);
					}
					log.debug("tasks size:" + tasks.size());
					tasks.clear();
					ObjectMapper objMapper = new ObjectMapper();
					tinfo.setIsresultavailable(true);
					outputqueue.offer(objMapper.writeValueAsBytes(tinfo));
				} else {
					Thread.sleep(1000);
				}
			}
			tes = zo.getTaskExectorsByJobId(teid);			
			if(isdriverallocated) {
				tes.addAll(zo.getDriversByJobId(teid));
			}
			tes.stream().forEach(hp->destroyTaskExecutors(hp, teid));			
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	public void destroyTaskExecutors(String hosthp, String jobid) {
		
			try {
				TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();				
				Utils.getResultObjectByInput(hosthp, taskExecutorshutdown, jobid);
				log.debug("The chamber case for container {} shattered for the port {} ", jobid, hosthp);
			} catch (Exception ex) {
				log.error("Destroy failed for the job " + jobid, ex);
			}
		
	}
	
	public void broadcastJobStageToTaskExecutors(List<Task> tasks) throws Exception {
		Kryo kryo = Utils.getKryoInstance();
		if (nonNull(pipelineconfig.getClsloader())) {
			kryo.setClassLoader(pipelineconfig.getClsloader());
		}
		final String finaljobid = teid;
		Map<String, Set<String>> jobexecutorsmap = tasks.stream()
				.collect(Collectors.groupingBy(task -> task.jobid + task.stageid, HashMap::new,
						Collectors.mapping(task -> task.hostport, Collectors.toSet())));
		jobexecutorsmap.keySet().stream().forEach(key -> {
			try {
				JobStage js = (JobStage) jsidjsmap.get(key);
				if (nonNull(js)) {
					js.setTejobid(finaljobid);				
					for (String te : jobexecutorsmap.get(key)) {
						if(nonNull(pipelineconfig.getJar())) {
							Utils.getResultObjectByInput(te, js, finaljobid, DataSamudayaMapReducePhaseClassLoader.newInstance(pipelineconfig.getJar(), Thread.currentThread().getContextClassLoader()));
						} else {
							Utils.getResultObjectByInput(te, js, finaljobid);
						}
					}
				}
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
	}
	
	public class AkkaActorsScheduler implements TaskProvider<StreamPipelineTaskSubmitter, Boolean> {
		Logger log = LoggerFactory.getLogger(AkkaActorsScheduler.class);
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Map<String, Semaphore> semaphores;
		AtomicBoolean shouldcontinueprocessing;

		public AkkaActorsScheduler(double totaltasks, Map<String, Semaphore> semaphores,
				AtomicBoolean shouldcontinueprocessing) {
			this.totaltasks = totaltasks;
			this.semaphores = semaphores;
			this.shouldcontinueprocessing = shouldcontinueprocessing;
		}

		Semaphore printresult = new Semaphore(1);

		ConcurrentMap<String, Timer> jobtimer = new ConcurrentHashMap<>();

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean> provideTask(
				final StreamPipelineTaskSubmitter spts) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean>() {
				private static final long serialVersionUID = 1L;

				@Override
				public Boolean execute() {
					try {
						semaphores.get(spts.getTask().getHostport()).acquire();
						if (!spts.isCompletedexecution() && shouldcontinueprocessing.get()) {
							spts.setTaskexecutors(zoglobal.getTaskExectorsByJobId(teid));
							Task result = (Task) spts.actors();
							log.debug("Task Status for task {} is {}", result.getTaskid(), result.taskstatus);
							printresult.acquire();
							counttaskscomp++;
							double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
							printresult.release();
							if (result.taskstatus == TaskStatus.FAILED) {
								spts.getTask().setTaskstatus(TaskStatus.FAILED);
								spts.getTask().stagefailuremessage = result.stagefailuremessage;
								spts.setCompletedexecution(false);
								shouldcontinueprocessing.set(false);
								throw new IllegalArgumentException("Task Failed");
							} else if (result.taskstatus == TaskStatus.COMPLETED) {
								spts.setCompletedexecution(true);
							}
							spts.getTask().setPiguuid(result.getPiguuid());
							return true;
						} else if (spts.isCompletedexecution() && shouldcontinueprocessing.get()) {
							printresult.acquire();
							double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
							printresult.release();
						}
					} catch (IllegalArgumentException e) {
						throw e;
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
						spts.setCompletedexecution(false);
					} finally {
						semaphores.get(spts.getTask().getHostport()).release();
					}
					return spts.isCompletedexecution();

				}
			};
		}

	}
	/**
	 * The method returns thread pool for the given number of tasks. 
	 * @param numberoftasks
	 * @return thread pool
	 */
	private ExecutorService newExecutor(int numberoftasks) {
		return Executors.newFixedThreadPool(numberoftasks, Thread.ofVirtual().factory());
	}
	
	/**
	 * The SQL execution akka actors
	 * @param origgraph
	 * @return modified graph
	 * @throws Exception
	 */
	public Graph<StreamPipelineTaskSubmitter, DAGEdge> parallelExecutionAkkaActors(
			final Graph<StreamPipelineTaskSubmitter, DAGEdge> origgraph) throws Exception {
		ExecutorService es = null;
		ExecutorService esroot = null;
		var lineagegraph = origgraph;
		try {
			var completed = false;
			var numexecute = 0;
			var executioncount = 1;
			List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> successresult = null;
			es = newExecutor(Runtime.getRuntime().availableProcessors());
			ExecutionResults<StreamPipelineTaskSubmitter, Boolean> executionresultscomplete = null;
			int batchsize = 0;

			new ConcurrentHashMap<String, List<StreamPipelineTaskSubmitter>>();
			var semaphores = new ConcurrentHashMap<String, Semaphore>();
			var tes = zoglobal.getTaskExectorsByJobId(teid);
			var cpu = Runtime.getRuntime().availableProcessors();
			for (var te : tes) {
				batchsize += cpu;
				semaphores.put(te, new Semaphore(cpu));
			}
			es = newExecutor(batchsize);
			while (!completed && numexecute < executioncount) {
				var shouldcontinueprocessing = new AtomicBoolean(true);

				Graph<StreamPipelineTaskSubmitter, DAGEdge> graphreversed = new EdgeReversedGraph<StreamPipelineTaskSubmitter, DAGEdge>(
						lineagegraph);
				TopologicalOrderIterator<StreamPipelineTaskSubmitter, DAGEdge> iterator = new TopologicalOrderIterator(
						graphreversed);
				String jobid;
				if (nonNull(pipelineconfig) && pipelineconfig.getUseglobaltaskexecutors()) {
					jobid = pipelineconfig.getTejobid();
				} else {
					jobid = origgraph.vertexSet().iterator().next().getTask().getJobid();
				}
				List<StreamPipelineTaskSubmitter> sptss = new ArrayList<>();
				while (iterator.hasNext()) {
					StreamPipelineTaskSubmitter sptsreverse = iterator.next();
					var predecessors = Graphs.predecessorListOf(graphreversed, sptsreverse);
					var successors = Graphs.successorListOf(graphreversed, sptsreverse);
					if (CollectionUtils.isEmpty(predecessors)) {
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), null, nonNull(sptsreverse.getTask().parentterminatingsize)?sptsreverse.getTask().parentterminatingsize:successors.size());
						Task task = (Task) Utils.getResultObjectByInput(sptsreverse.getHostPort(), gettaskactor, jobid);
						sptsreverse.getTask().setActorselection(task.getActorselection());
					} else {
						var childactorsoriggraph = predecessors.stream().map(spts -> spts.getTask().getActorselection())
								.collect(Collectors.toList());
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), childactorsoriggraph,
								nonNull(sptsreverse.getTask().parentterminatingsize)?sptsreverse.getTask().parentterminatingsize:successors.size());
						Task task = (Task) Utils.getResultObjectByInput(sptsreverse.getHostPort(), gettaskactor, jobid);
						sptsreverse.getTask().setActorselection(task.getActorselection());
						sptsreverse.setChildactors(childactorsoriggraph);
					}
					if (CollectionUtils.isEmpty(successors)) {
						sptss.add(sptsreverse);
					}
				}
				var configexec = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(es, new AkkaActorsScheduler(
						Double.valueOf("" + sptss.size()), semaphores, shouldcontinueprocessing));
				var dexecutor = new DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>(configexec);
				sptss.stream().forEach(dexecutor::addIndependent);
				executionresultscomplete = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
				successresult = executionresultscomplete.getSuccess();
				if (sptss.size() == successresult.size()) {
					completed = true;
				} else {
					throw new PipelineException("Error In Executing Tasks");
				}
				numexecute++;
			}
			if (!completed) {
				StringBuilder sb = new StringBuilder();
				if (executionresultscomplete != null) {
					executionresultscomplete.getErrored().forEach(spts -> {
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append(spts.getId().getTask().stagefailuremessage);
					});
					if (!sb.isEmpty()) {
						throw new PipelineException(
								PipelineConstants.ERROREDTASKS.replace("%s", "" + executioncount) + sb.toString());
					}
				}
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
			if (!Objects.isNull(esroot)) {
				esroot.shutdownNow();
				esroot.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
		return lineagegraph;
	}
	/**
	 * Set App Master service hosts and port running before the container is
	 * launched.
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
				log.debug("App Master Service Ip Address: " + address);
				log.debug("App Master Service Port: " + port);
				env = new HashMap<>(context.getEnvironment());
				// Set the service port to the environment object.
				env.put(YarnSystemConstants.AMSERVICE_PORT, Integer.toString(port));

				// Set the service host to the environment object.
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
	 * Execute the OnContainer completed method when container is exited with the
	 * exitcode.
	 */
	@Override
	protected void onContainerCompleted(ContainerStatus status) {
		synchronized (lock) {
			log.debug("Before Container {} completed: {} {} {}", status.getContainerId(), status.getContainerSubState(), status.getExitStatus(), status.getState());
			status.setState(ContainerState.COMPLETE);
			status.setContainerSubState(ContainerSubState.DONE);
			status.setExitStatus(0);
			super.onContainerCompleted(status);
			log.debug("Container completed: " + status.getContainerId());
			if (containertaskmap.get(status.getContainerId().toString()) != null) {
				var jobidstageidjs = containertaskmap.get(status.getContainerId().toString());
				pendingtasks.keySet().removeAll(jobidstageidjs.keySet());
			}
		}
	}

	/**
	 * Execute the OnContainer failed method when container is exited with the
	 * exitcode.
	 */
	@Override
	protected boolean onContainerFailed(ContainerStatus status) {
		synchronized (lock) {
			log.debug("Container failed: " + status.getContainerId());
			if (containertaskmap.get(status.getContainerId().toString()) != null) {
				pendingtasks.putAll(containertaskmap.get(status.getContainerId().toString()));
			}
			if (hasJobs()) {
				log.debug("Container failed: " + "Has jobs reallocating container for: " + status.getContainerId());
				getAllocator().allocateContainers(1);
			}
			return true;
		}
	}

	/**
	 * Check whether the job has been completed by the containers and app master to
	 * exit.
	 */
	@Override
	protected boolean isComplete() {
		return completedJobs();
	}

	/**
	 * Check on whether the jobs are available to execute.
	 * 
	 * @return true when jobs available and container not to kill
	 */
	public boolean hasJobs() {
		synchronized (lock) {			
			boolean hasJobs = isreadytoexecute
					&& (tasks.size() > 0);
			log.debug("Has Jobs: {}",hasJobs);
			return hasJobs || !tokillcontainers;

		}
	}

	/**
	 * The method returns taskexecutor id for running sql queries
	 * @return taskexecutorid
	 */
	public String getTaskExecutorId() {
		return teid;
	}
	
	/**
	 * Get executor type as driver or executor 
	 * @return executor type
	 */
	public EXECUTORTYPE getExecutorType() {
		try {
			executoralloclock.acquire();
			if(isdriverrequired && !isdriverallocated) {
				isdriverallocated = true;
				return EXECUTORTYPE.DRIVER;
			}
			return EXECUTORTYPE.EXECUTOR;
		} catch (Exception e) {
			log.error("", e);
		} finally {
			executoralloclock.release();
		}
		return EXECUTORTYPE.EXECUTOR;
	}
	
	/**
	 * Check whether the jobs has been completed.
	 * 
	 * @return
	 */
	private boolean completedJobs() {
		synchronized (lock) {			
			return tokillcontainers;
		}
	}

	@Override
	public String toString() {
		return DataSamudayaConstants.PENDINGJOBS + DataSamudayaConstants.EQUAL + pendingtasks.size() + DataSamudayaConstants.SINGLESPACE
				+ DataSamudayaConstants.RUNNINGJOBS + DataSamudayaConstants.EQUAL + pendingtasks.size();
	}
}
