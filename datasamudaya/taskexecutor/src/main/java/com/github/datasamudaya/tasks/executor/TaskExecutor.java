/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor;

import java.io.File;
import java.io.OutputStream;
import java.net.URI;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.h2.util.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.CloseStagesGraphExecutor;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.FreeResourcesCompletedJob;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.ReducerValues;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.RetrieveData;
import com.github.datasamudaya.common.RetrieveKeys;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.common.TasksGraphExecutor;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutor;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorInMemory;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorInMemoryDisk;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorInMemoryDiskSQL;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorJGroups;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorJGroupsSQL;

import static java.util.Objects.isNull;

/**
 * The executor that executes task executor tasks. 
 * @author arun
 *
 */
public class TaskExecutor implements Callable<Object> {
	private static final Logger log = LoggerFactory.getLogger(TaskExecutor.class);
	int port;
	ExecutorService es;
	ConcurrentMap<String, OutputStream> resultstream;
	Configuration configuration;
	Map<String, Object> apptaskexecutormap;
	Cache inmemorycache;
	Object deserobj;
	Map<String, Object> jobstageexecutormap;
	Map<String, Map<String, Object>> jobidstageidexecutormap;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	Map<String, JobStage> jobidstageidjobstagemap;
	Task tasktoreturn;
	ZookeeperOperations zo;
	ConcurrentMap<BlocksLocation, String> blorcmap;
	boolean topersist;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutor(ClassLoader cl, int port, ExecutorService es, Configuration configuration,
			Map<String, Object> apptaskexecutormap, Map<String, Object> jobstageexecutormap,
			ConcurrentMap<String, OutputStream> resultstream, Cache inmemorycache, Object deserobj,

			Map<String, Map<String, Object>> jobidstageidexecutormap, Task tasktoreturn,
			Map<String, JobStage> jobidstageidjobstagemap,
			ZookeeperOperations zo, ConcurrentMap<BlocksLocation, String> blorcmap,
			Map<String, Boolean> jobidstageidtaskidcompletedmap) {
		this.cl = cl;
		this.port = port;
		this.es = es;
		this.configuration = configuration;
		this.apptaskexecutormap = apptaskexecutormap;
		this.resultstream = resultstream;
		this.inmemorycache = inmemorycache;
		this.jobstageexecutormap = jobstageexecutormap;
		this.deserobj = deserobj;
		this.jobidstageidexecutormap = jobidstageidexecutormap;
		this.tasktoreturn = tasktoreturn;
		this.jobidstageidjobstagemap = jobidstageidjobstagemap;
		this.zo = zo;
		this.blorcmap = blorcmap;
		this.topersist = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOPERSISTYOSEGICOLUMNAR, DataSamudayaConstants.TOPERSISTYOSEGICOLUMNAR_DEFAULT));
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
	}

	ClassLoader cl;

	/**
	* initializes the zo.
	* @param zo
	* @param jobid
	* @throws Exception
	*/
	public void init(ZookeeperOperations zo, String jobid) throws Exception {

		var host = NetworkUtil
				.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		var port = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT);

		var hp = host + DataSamudayaConstants.UNDERSCORE + port;

		zo.createTaskExecutorNode(jobid, hp, DataSamudayaConstants.EMPTY.getBytes(), event -> {
			log.info("TaskExecutor {} initialized and started", hp);
		});

	}

	/**
	 * Executes the tasks and return the results.
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public Object call() {
		log.debug("Started the run------------------------------------------------------");
		try {
			if (deserobj instanceof JobStage jobstage) {
				log.info("JobStage {} with port {} ", jobstage, port);
				jobidstageidjobstagemap.put(jobstage.getJobid() + jobstage.getStageid(), jobstage);
				tasktoreturn.taskstatus = TaskStatus.COMPLETED;
				tasktoreturn.tasktype = TaskType.JOBSTAGEMAPPUT;
				tasktoreturn.stagefailuremessage = "Acquired job stage: " + jobstage;
				init(zo, jobstage.getJobid());
				return tasktoreturn;
			} else if (deserobj instanceof Task task) {
				log.info("Acquired Task: " + task);
				var taskexecutor = jobstageexecutormap.get(task.jobid + task.stageid + task.taskid);
				var spte = (StreamPipelineTaskExecutor) taskexecutor;
				if (taskexecutor == null
						|| !spte.isCompleted() && task.taskstatus == TaskStatus.FAILED) {
					var key = task.jobid + task.stageid;
					spte = task.storage == STORAGE.INMEMORY_DISK
							? new StreamPipelineTaskExecutorInMemoryDisk(jobidstageidjobstagemap.get(key),
							resultstream, inmemorycache)
							: task.storage == STORAGE.INMEMORY
							? new StreamPipelineTaskExecutorInMemory(jobidstageidjobstagemap.get(key),
							resultstream, inmemorycache)
							: task.storage == STORAGE.COLUMNARSQL
							? new StreamPipelineTaskExecutorInMemoryDiskSQL(jobidstageidjobstagemap.get(key),
							resultstream, inmemorycache, task.isTopersist())
							: new StreamPipelineTaskExecutor(jobidstageidjobstagemap.get(key), inmemorycache);
					spte.setTask(task);
					spte.setExecutor(es);
					jobstageexecutormap.remove(key + task.taskid);
					jobstageexecutormap.put(key + task.taskid, spte);
					Map<String, Object> stageidexecutormap;
					if (Objects.isNull(jobidstageidexecutormap.get(task.jobid))) {
						stageidexecutormap = new ConcurrentHashMap<>();
						jobidstageidexecutormap.put(task.jobid, stageidexecutormap);
					} else {
						stageidexecutormap = (Map<String, Object>) jobidstageidexecutormap.get(task.jobid);
					}
					stageidexecutormap.put(task.stageid, spte);
					log.info("Submitted kickoff execution: " + deserobj);
					boolean status = spte.call();
					log.info("Is Task Completed: {}", status);
					tasktoreturn.taskstatus = status ? TaskStatus.COMPLETED : TaskStatus.FAILED;
					tasktoreturn.tasktype = TaskType.EXECUTEUSERTASK;
					tasktoreturn.taskid = task.taskid;
					tasktoreturn.piguuid = task.piguuid;
					tasktoreturn.taskexecutionstartime = task.taskexecutionstartime;
					tasktoreturn.taskexecutionendtime = task.taskexecutionendtime;
					tasktoreturn.timetakenseconds = task.timetakenseconds;
					tasktoreturn.numbytesprocessed = task.numbytesprocessed;
					tasktoreturn.numbytesconverted = task.numbytesconverted;
					tasktoreturn.numbytesgenerated = task.numbytesgenerated;
					if (tasktoreturn.taskstatus == TaskStatus.FAILED) {
						tasktoreturn.stagefailuremessage = task.stagefailuremessage;
					}
					return tasktoreturn;
				}
			} else if (deserobj instanceof TasksGraphExecutor stagesgraph) {
				int numoftasks = stagesgraph.getTasks().size();
				var key = stagesgraph.getTasks().get(numoftasks - 1).jobid
						+ stagesgraph.getTasks().get(numoftasks - 1).stageid
						+ stagesgraph.getTasks().get(numoftasks - 1).taskid;
				var taskexecutor = jobstageexecutormap.get(key);
				if (taskexecutor == null) {
					log.info("In JGroups Port And Tasks {} {}", port, stagesgraph.getTasks());
					StreamPipelineTaskExecutor sptej = null;
					if (stagesgraph.getStorage() == STORAGE.COLUMNARSQL) {
						sptej = new StreamPipelineTaskExecutorJGroupsSQL(jobidstageidjobstagemap, stagesgraph.getTasks(), port,
								inmemorycache, blorcmap, stagesgraph.getTasks().get(0).isTopersist());
						log.info("In JGroups Storage Columnar Object {}", sptej);
					} else if (stagesgraph.getStorage() == STORAGE.DISK) {
						sptej = new StreamPipelineTaskExecutorJGroups(jobidstageidjobstagemap, stagesgraph.getTasks(), port,
								inmemorycache);
						log.info("In JGroups Storage Object {}", sptej);
					}
					sptej.setExecutor(es);
					for (Task task : stagesgraph.getTasks()) {
						jobstageexecutormap.put(task.jobid + task.stageid + task.taskid, sptej);
					}
					es.submit(sptej);
					tasktoreturn.tasktype = TaskType.JGROUPSTASKGRAPHEXECUTE;
					tasktoreturn.taskstatus = TaskStatus.SUBMITTED;
					return tasktoreturn;
				}
			} else if (deserobj instanceof CloseStagesGraphExecutor closestagesgraph) {
				var key = closestagesgraph.getTasks().get(0).jobid + closestagesgraph.getTasks().get(0).stageid
						+ closestagesgraph.getTasks().get(0).taskid;
				var taskexecutor = jobstageexecutormap.get(key);
				if (taskexecutor != null) {
					if (taskexecutor instanceof StreamPipelineTaskExecutorJGroupsSQL sptejs) {
						sptejs.channel.close();
					} else if (taskexecutor instanceof StreamPipelineTaskExecutorJGroups spte) {
						spte.channel.close();
					}
					for (Task task : closestagesgraph.getTasks()) {
						jobstageexecutormap.remove(task.jobid + task.stageid + task.taskid);
						File todelete = new File(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR)
								+ DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + task.jobid
								+ DataSamudayaConstants.FORWARD_SLASH + task.taskid);
						todelete.delete();
						log.info("Sanitize the task " + todelete);
					}
					File jobtmpdir = new File(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR) + DataSamudayaConstants.FORWARD_SLASH
							+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + closestagesgraph.getTasks().get(0).jobid);
					File[] taskintermediatefiles = jobtmpdir.listFiles();
					if (jobtmpdir.isDirectory()) {
						if (Objects.isNull(taskintermediatefiles)
								|| Objects.nonNull(taskintermediatefiles) && taskintermediatefiles.length == 0) {
							jobtmpdir.delete();
						}
					}
					tasktoreturn.tasktype = TaskType.RELEASERESOURCESJGROUPS;
					tasktoreturn.taskstatus = TaskStatus.COMPLETED;
				}
			} else if (deserobj instanceof FreeResourcesCompletedJob cce) {
				var jsem = jobidstageidexecutormap.remove(cce.getJobid());
				if (!Objects.isNull(jsem)) {
					var keys = jsem.keySet();
					for (var key : keys) {
						jsem.remove(key);
						jobstageexecutormap.remove(cce.getJobid() + key);
						log.info("Sanitize stages: " + cce.getJobid() + key);
					}
				}
				tasktoreturn.tasktype = TaskType.FREERESOURCES;
				tasktoreturn.taskstatus = TaskStatus.COMPLETED;
			} else if (deserobj instanceof RemoteDataFetch rdf) {
				var taskexecutor =
						jobstageexecutormap.get(rdf.getJobid() + rdf.getStageid() + rdf.getTaskid());
				var mdstde = (StreamPipelineTaskExecutor) taskexecutor;
				log.info("Task Executor Remote Data Fetch {}", taskexecutor);
				if (rdf.getMode().equals(DataSamudayaConstants.STANDALONE)) {
					if (rdf.getStorage() == STORAGE.COLUMNARSQL) {
						var path = Utils.getIntermediateInputStreamRDF(rdf);
						while (isNull(jobidstageidtaskidcompletedmap.get(path)) || !jobidstageidtaskidcompletedmap.get(path)) {
							Thread.sleep(1000);
						}
						rdf.setData((byte[]) inmemorycache.get(path));
						return rdf;
					} else if (taskexecutor != null) {
						Task task = mdstde.getTask();
						if (task.storage == DataSamudayaConstants.STORAGE.INMEMORY) {
							var os = ((StreamPipelineTaskExecutorInMemory) mdstde).getIntermediateInputStreamRDF(rdf);
							log.info("Intermediate InputStream RDF {}", os);
							if (!Objects.isNull(os)) {
								ByteBufferOutputStream bbos = (ByteBufferOutputStream) os;
								rdf.setData(
										IOUtils.readBytesAndClose(new ByteBufferInputStream(bbos.get()), bbos.get().limit()));
							}
						} else if (task.storage == DataSamudayaConstants.STORAGE.INMEMORY_DISK) {
							var path = Utils.getIntermediateInputStreamRDF(rdf);
							while (!jobidstageidtaskidcompletedmap.get(path)) {
								Thread.sleep(1000);
							}
							rdf.setData((byte[]) inmemorycache.get(path));
						} else {
							try (var is = mdstde.getIntermediateInputStreamFS(task);) {
								rdf.setData((byte[]) is.readAllBytes());
							}
						}
						return rdf;
					}
				} else if (rdf.getMode().equals(DataSamudayaConstants.JGROUPS)) {
					try (var is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.getJobid(),
							rdf.getTaskid());) {
						rdf.setData((byte[]) is.readAllBytes());
						return rdf;
					}
				} else {
					if (taskexecutor != null) {
						try (var is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(rdf.getJobid(),
								mdstde.getIntermediateDataRDF(rdf.getTaskid()));) {
							rdf.setData((byte[]) is.readAllBytes());
							return rdf;
						}
					}
				}
			} else if (deserobj instanceof List objects) {
				var object = objects.get(0);
				var applicationid = (String) objects.get(1);
				var taskid = (String) objects.get(2);
				{
					var apptaskid = applicationid + taskid;
					var taskexecutor = apptaskexecutormap.get(apptaskid);
					if (object instanceof BlocksLocation blockslocation) {
						var mdtemc = (TaskExecutorMapperCombiner) taskexecutor;
						if (taskexecutor == null) {
							try (var hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get()
											.getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)),
									configuration)) {
								log.info("Application Submitted:" + applicationid + "-" + taskid);
								mdtemc = new TaskExecutorMapperCombiner(blockslocation,
										HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs), applicationid,
										taskid, cl, port);
								apptaskexecutormap.put(apptaskid, mdtemc);
								mdtemc.call();
								var keys = mdtemc.ctx.keys();
								RetrieveKeys rk = new RetrieveKeys();
								rk.keys = new LinkedHashSet<>(keys);
								rk.applicationid = applicationid;
								rk.taskid = taskid;
								rk.response = true;
								log.info("destroying MapperCombiner {}", apptaskid);
								return rk;
							}
						}
					} else if (object instanceof ReducerValues rv) {
						var executorid = (String) objects.get(3);
						var mdter = (TaskExecutorReducer) taskexecutor;
						if (taskexecutor == null) {
							mdter =
									new TaskExecutorReducer(rv, applicationid, taskid, cl, port, apptaskexecutormap, executorid);
							apptaskexecutormap.put(apptaskid, mdter);
							log.debug("Reducer submission:" + apptaskid);
							return mdter.call();
						}
					} else if (object instanceof RetrieveData) {
						Context ctx = null;
						if (taskexecutor instanceof TaskExecutorReducer ter) {
							log.info("Gathering reducer context: " + apptaskid);
							ctx = ter.ctx;
						} else if (taskexecutor instanceof TaskExecutorMapperCombiner temc) {
							log.info("Gathering reducer context: " + apptaskid);
							ctx = temc.ctx;
						}
						apptaskexecutormap.remove(apptaskid);
						return ctx;
					} else if (object instanceof RetrieveKeys rk) {
						var mdtemc = (TaskExecutorMapperCombiner) taskexecutor;
						var keys = mdtemc.ctx.keys();
						rk.keys = new LinkedHashSet<>(keys);
						rk.applicationid = applicationid;
						rk.taskid = taskid;
						rk.response = true;
						log.debug("destroying MapperCombiner HeartBeat: " + apptaskid);
						return rk;
					}
				}
			}
		} catch (Exception ex) {
			log.error("Job Execution Problem", ex);
			Utils.getStackTrace(ex, tasktoreturn);
		}
		return tasktoreturn;

	}
}
