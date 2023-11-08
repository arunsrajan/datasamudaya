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

import java.net.InetAddress;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.hadoop.yarn.api.records.Container;
import org.apache.hadoop.yarn.api.records.ContainerLaunchContext;
import org.apache.hadoop.yarn.api.records.ContainerStatus;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.am.ContainerLauncherInterceptor;
import org.springframework.yarn.am.StaticEventingAppmaster;
import org.springframework.yarn.am.allocate.DefaultContainerAllocator;
import org.springframework.yarn.am.container.AbstractLauncher;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskInfoYARN;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.scheduler.StreamPipelineTaskSubmitter;

/**
 * 
 * @author Arun 
 * Yarn App master with lifecycle init, submitapplication,
 * isjobcomplete and prelaunch containers. Various container events
 * captured are container failure and completed operation with container
 * statuses.
 */
public class StreamPipelineYarnAppmaster extends StaticEventingAppmaster implements ContainerLauncherInterceptor {

	private static final Log log = LogFactory.getLog(StreamPipelineYarnAppmaster.class);
	private List<Task> tasks = new Vector<>();

	private Map<String, Task> pendingtasks = new ConcurrentHashMap<>();
	private Map<String, Task> pendingsubmittedtasks = new ConcurrentHashMap<>();
	private Map<String, Timer> requestresponsetimer = new ConcurrentHashMap<>();
	private Map<String, Map<String, Task>> containertaskmap = new ConcurrentHashMap<>();
	private Map<String, JobStage> jsidjsmap;
	private Map<String, Boolean> sentjobstages = new ConcurrentHashMap<>();
	TaskInfoYARN tinfo = new TaskInfoYARN();
	private final Object lock = new Object();
	SimpleDistributedQueue outputqueue = null;

	private long taskidcounter;
	private long taskcompleted;
	private boolean tokillcontainers = false;
	private boolean isreadytoexecute = false;
	private List<StreamPipelineTaskSubmitter> mdststs;
	private SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph = new SimpleDirectedGraph<>(
			DAGEdge.class);
	private Map<String, StreamPipelineTaskSubmitter> taskmdsthread;

	/**
	 * Container initialization.
	 */
	@Override
	protected void onInit() throws Exception {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
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
			es = Executors.newFixedThreadPool(1);
			es.execute(()->pollQueue());
			var prop = new Properties();
			DataSamudayaProperties.put(prop);
			ByteBufferPoolDirect.init(2*DataSamudayaConstants.GB);	
			var containerallocator = (DefaultContainerAllocator) getAllocator();
			log.debug("Parameters: " + getParameters());
			log.info("Container-Memory: " + getParameters().getProperty("container-memory", "1024"));
			log.info("Container-Cpu: " + getParameters().getProperty("container-cpu", "1"));
			containerallocator.setVirtualcores(Integer.parseInt(getParameters().getProperty("container-cpu", "1")));
			containerallocator.setMemory(Integer.parseInt(getParameters().getProperty("container-memory", "1024")));
		} catch (Exception ex) {
			log.debug("Submit Application Error, See cause below \n", ex);
		}
		var appmasterservice = (StreamPipelineYarnAppmasterService) getAppmasterService();
		log.debug("In SubmitApplication Setting AppMaster Service: " + appmasterservice);
		if (appmasterservice != null) {
			// Set the Yarn App master bean to the Yarn App master service object.
			appmasterservice.setYarnAppMaster(this);
		}
		super.submitApplication();
	}
	Map<String, String> containeridipmap = new ConcurrentHashMap<>();

	@SuppressWarnings("unchecked")
	protected void pollQueue(){
		log.debug("Task Id Counter: " + taskidcounter);
		log.debug("Environment: " + getEnvironment());
		try(var zo = new ZookeeperOperations();){
	 	zo.connect();
	 	String teid = getEnvironment().get(DataSamudayaConstants.YARNDATASAMUDAYAJOBID);
		SimpleDistributedQueue inputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
				+ DataSamudayaConstants.YARN_INPUT_QUEUE
				+ DataSamudayaConstants.FORWARD_SLASH + teid);
		
		outputqueue = zo.createDistributedQueue(DataSamudayaConstants.ROOTZNODEZK
				+ DataSamudayaConstants.YARN_OUTPUT_QUEUE
				+ DataSamudayaConstants.FORWARD_SLASH + teid);
		
		ObjectMapper objectMapper = new ObjectMapper();
		while(!tokillcontainers) {
			if (inputqueue.peek() != null && !isreadytoexecute) {
				pendingtasks.clear();
				pendingsubmittedtasks.clear();
				containertaskmap.clear();
				sentjobstages.clear();
				taskidcounter = 0;
				tinfo = objectMapper.readValue(inputqueue.poll(), TaskInfoYARN.class);
				tokillcontainers = tinfo.isTokillcontainer();
				if(Objects.isNull(tinfo.getJobid())) {
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
				tasks = mdststs.stream().map(StreamPipelineTaskSubmitter::getTask).collect(Collectors.toCollection(Vector::new));
				isreadytoexecute = true;
				log.debug("tasks size:" + tasks.size());
			} else {
				Thread.sleep(1000);
			}
		}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);			
		}
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
			super.onContainerCompleted(status);
			log.debug("Container completed: " + status.getContainerId());
			if (containertaskmap.get(status.getContainerId().toString()) != null) {
				var jobidstageidjs = containertaskmap.get(status.getContainerId().toString());
				pendingtasks.keySet().removeAll(jobidstageidjs.keySet());
			}
			if (hasJobs()) {
				log.debug("Container completed: " + "Has jobs reallocating container for: " + status.getContainerId());
				getAllocator().allocateContainers(1);
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
		return completedJobs() && super.isComplete();
	}

	/**
	 * Update the job statuses if job status is completed.
	 *
	 * @param task
	 * @param success
	 * @param containerid
	 */
	@SuppressWarnings("ucd")
	public void reportJobStatus(Task task, boolean success, String containerid) {
		synchronized (lock) {
			if (success) {
				taskcompleted++;
				log.debug(task.jobid + task.stageid + task.taskid + " Updated");
				taskmdsthread.get(task.jobid + task.stageid + task.taskid).setCompletedexecution(true);
				pendingtasks.remove(task.jobid + task.stageid + task.taskid);
				containertaskmap.get(containerid).remove(task.jobid + task.stageid + task.taskid);
			} else {
				pendingtasks.put(task.jobid + task.stageid + task.taskid, task);
			}
			pendingsubmittedtasks.remove(task.jobid + task.stageid + task.taskid);
		}
	}

	/**
	 * Obtain the job to execute
	 * 
	 * @return
	 */
	public Object getTask(String containerid) {
		synchronized (lock) {
			if(isreadytoexecute) {
				if (!sentjobstages.containsKey(containerid)) {
					sentjobstages.put(containerid, true);
					return jsidjsmap;
				}
				// If the pending jobs being staged first execute the pending jobs.
				if (pendingtasks.size() > 0) {
					var pendings = pendingtasks.keySet();
					for (var pend : pendings) {
						if (!pendingsubmittedtasks.containsKey(pend)) {
							pendingsubmittedtasks.put(pend, pendingtasks.get(pend));
							var timer = new Timer();
							var task = pendingtasks.get(pend);
							requestresponsetimer.put(task.jobid + task.stageid + task.taskid, timer);
							timer.schedule(new JobStageTimerTask(task, pendingtasks, pendingsubmittedtasks, lock,
									requestresponsetimer), 10000);
							return task;
						}
					}
	
				}
				// If the jobs stage submitted is less than the total jobs size
				// return the job to be executed.
				if (taskidcounter < tasks.size()) {
					var task = tasks.get((int) taskidcounter);
					String ip = containeridipmap.get(containerid.trim());
					if (!Objects.isNull(task.input)) {
						if (task.input[0] instanceof BlocksLocation) {
							var bl = (BlocksLocation) task.input[0];
							if (!Objects.isNull(bl.getBlock()) && bl.getBlock().length > 0) {
								String[] blockip = bl.getBlock()[0].getHp().split(DataSamudayaConstants.COLON);
								if (!ip.equals(blockip[0])) {
									return null;
								}
							}
						}
					}
					var toexecute = true;
					var predessorslist = Graphs.predecessorListOf(graph, taskmdsthread.get(task.jobid + task.stageid + task.taskid));
					for (var succcount = 0; succcount < predessorslist.size(); succcount++) {
						var predthread = predessorslist.get(succcount);
						if (!taskmdsthread.get(predthread.getTask().jobid + predthread.getTask().stageid + predthread.getTask().taskid)
								.isCompletedexecution()) {
							toexecute = false;
							break;
						}
	
					}
					if (toexecute) {
						if (!containertaskmap.containsKey(containerid)) {
							containertaskmap.put(containerid, new ConcurrentHashMap<>());
						}
						containertaskmap.get(containerid).put(task.jobid + task.stageid + task.taskid, task);
						log.debug(
								"Allocating JobsStage " + task.jobid + task.stageid + task.taskid + " to the container: " + containerid);
						taskidcounter++;
						var timer = new Timer();
						requestresponsetimer.put(task.jobid + task.stageid + task.taskid, timer);
						timer.schedule(new JobStageTimerTask(task, pendingtasks, pendingsubmittedtasks, lock,
								requestresponsetimer), 10000);
						log.debug(task + " To Execute, Task Counter:" + taskidcounter);
						return task;
					} else {
						log.debug(task + " To StandBy, Task Counter:" + taskidcounter);
						return null;
					}
				}
			}
			return null;
		}
	}

	public void requestRecieved(Task task) {
		synchronized (lock) {
			var timer = requestresponsetimer.get(task.jobid + task.stageid + task.taskid);
			timer.cancel();
			timer.purge();
			requestresponsetimer.remove(task.jobid + task.stageid + task.taskid);
		}
	}

	/**
	 * Check on whether the jobs are available to execute.
	 * 
	 * @return true when jobs available and container not to kill
	 */
	public boolean hasJobs() {
		synchronized (lock) {
			log.debug("Has Jobs: " + (taskidcounter < tasks.size()) + ", Task Counter:" + taskidcounter);
			boolean hasJobs = (isreadytoexecute
					&& (taskidcounter < tasks.size() || pendingtasks.size() > 0 || taskcompleted < tasks.size()));
			try {			
				if (tasks.size() > 0 && taskcompleted >= tasks.size()) {
					tasks.clear();
					ObjectMapper objMapper = new ObjectMapper();
					tinfo.setIsresultavailable(true);
					outputqueue.offer(objMapper.writeValueAsBytes(tinfo));
					taskcompleted = 0;
					isreadytoexecute = false;
				}
			} catch(Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
			return hasJobs || !tokillcontainers;

		}
	}

	/**
	 * Check whether the jobs has been completed.
	 * 
	 * @return
	 */
	private boolean completedJobs() {
		synchronized (lock) {
			log.debug("Completed Jobs: " + (taskcompleted >= tasks.size()) + ", Task Completeed Counter:"
					+ taskcompleted);
			return taskcompleted >= tasks.size();
		}
	}

	@Override
	public String toString() {
		return DataSamudayaConstants.PENDINGJOBS + DataSamudayaConstants.EQUAL + pendingtasks.size() + DataSamudayaConstants.SINGLESPACE
				+ DataSamudayaConstants.RUNNINGJOBS + DataSamudayaConstants.EQUAL + pendingtasks.size();
	}

	static class JobStageTimerTask extends TimerTask {
		private Task task;
		private Map<String, Task> pendingtasks;
		Object locktimer;
		private Map<String, Timer> requestresponsetimer;
		private Map<String, Task> pendingsubmittedtasks;

		private JobStageTimerTask(Task task, Map<String, Task> pendingtasks, Map<String, Task> pendingsubmittedtasks,
				Object locktimer, Map<String, Timer> requestresponsetimer) {
			this.task = task;
			this.pendingtasks = pendingtasks;
			this.locktimer = locktimer;
			this.requestresponsetimer = requestresponsetimer;
			this.pendingsubmittedtasks = pendingsubmittedtasks;
		}

		@Override
		public void run() {
			synchronized (locktimer) {
				var timer = requestresponsetimer.get(task.jobid + task.stageid + task.taskid);
				timer.cancel();
				timer.purge();
				log.debug("");
				pendingsubmittedtasks.remove(task.jobid + task.stageid + task.taskid);
				pendingtasks.put(task.jobid + task.stageid + task.taskid, task);
				log.debug("Response Not Received:" + task.jobid + task.stageid + task.taskid + " Putting In Pending Jobs");
			}
		}

	}
}
