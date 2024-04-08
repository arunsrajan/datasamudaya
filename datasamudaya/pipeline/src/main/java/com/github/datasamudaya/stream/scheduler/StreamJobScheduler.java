/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.stream.scheduler;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.net.InetSocketAddress;
import java.net.MalformedURLException;
import java.net.Socket;
import java.net.URI;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.calcite.rel.RelFieldCollation.Direction;
import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.ehcache.Cache;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.EdgeReversedGraph;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jgroups.JChannel;
import org.jgroups.ObjectMessage;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.YarnSystemConstants;
import org.springframework.yarn.client.CommandYarnClient;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Block;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.CleanupTaskActors;
import com.github.datasamudaya.common.CloseStagesGraphExecutor;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.ExecuteTaskActor;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.FreeResourcesCompletedJob;
import com.github.datasamudaya.common.GetTaskActor;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.Job.TRIGGER;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskInfoYARN;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TasksGraphExecutor;
import com.github.datasamudaya.common.TssHAChannel;
import com.github.datasamudaya.common.TssHAHostPorts;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.DistributedDistinct;
import com.github.datasamudaya.common.functions.DistributedSort;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.Join;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftJoin;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunctionValues;
import com.github.datasamudaya.common.functions.ReduceFunction;
import com.github.datasamudaya.common.functions.RightJoin;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.ShuffleStage;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.BTree;
import com.github.datasamudaya.common.utils.DataSamudayaMetricsExporter;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.FieldCollatedSortedComparator;
import com.github.datasamudaya.common.utils.IteratorType;
import com.github.datasamudaya.common.utils.RemoteIteratorClient;
import com.github.datasamudaya.common.utils.RequestType;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutor;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorIgnite;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorIgniteSQL;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorLocal;
import com.github.datasamudaya.stream.mesos.scheduler.MesosScheduler;
import com.github.datasamudaya.stream.pig.PigSortedComparator;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;
import com.github.dexecutor.core.task.TaskProvider;
import com.google.common.collect.Iterables;
import com.typesafe.config.Config;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import akka.cluster.Cluster;

/**
 * 
 * @author Arun Schedule the jobs for parallel execution to task executor for
 *         standalone application or mesos scheduler and executor or yarn
 *         scheduler i.e app master and executor.
 */
public class StreamJobScheduler {

	private static final Logger log = LoggerFactory.getLogger(StreamJobScheduler.class);

	public int batchsize;
	public Set<String> taskexecutors;
	private final Semaphore yarnmutex = new Semaphore(1);
	public static ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
	@SuppressWarnings("rawtypes")
	Cache cache;
	public Semaphore semaphore;

	public PipelineConfig pipelineconfig;
	AtomicBoolean istaskcancelled = new AtomicBoolean();
	public Map<String, JobStage> jsidjsmap = new ConcurrentHashMap<>();
	Map<String, Boolean> jobidstageidtaskidcompletedmap = new ConcurrentHashMap<>();
	Map<String, Object> actornameactorrefmap = new ConcurrentHashMap<>();
	public List<Object> stageoutput = new ArrayList<>();
	List<ActorRef> actorrefs = new ArrayList<>();
	String hdfsfilepath;
	FileSystem hdfs;
	String hbphysicaladdress;
	ActorSystem system;

	public StreamJobScheduler() {
		hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
	}

	ExecutorService jobping = Executors.newWorkStealingPool();
	public Job job;
	public Boolean islocal;
	public Boolean isignite, ismesos, isyarn, isjgroups;
	JChannel chtssha;
	SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph;
	public ZookeeperOperations zo;

	/**
	 * Schedule the job for parallelization
	 * 
	 * @param job
	 * @return
	 * @throws Exception
	 * @throws Throwable
	 */
	@SuppressWarnings({ "unchecked", "rawtypes", "resource" })
	public Object schedule(Job job) throws Exception {
		this.job = job;
		this.pipelineconfig = job.getPipelineconfig();
		// If scheduler is mesos?
		ismesos = Boolean.parseBoolean(pipelineconfig.getMesos());
		// If scheduler is yarn?
		isyarn = Boolean.parseBoolean(pipelineconfig.getYarn());
		// If scheduler is local?
		islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
		// If scheduler is JGroups
		var isjgroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
		// If ignite mode is choosen
		isignite = Objects.isNull(pipelineconfig.getMode()) ? false
				: pipelineconfig.getMode().equals(DataSamudayaConstants.MODE_DEFAULT) ? true : false;

		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			var zo = new ZookeeperOperations();
			zo.connect();
			this.zo = zo;
			this.hdfs = hdfs;
			TasksGraphExecutor[] tasksgraphexecutor = null;
			// If not yarn and mesos start the resources and task scheduler
			// heart beats
			// for standalone DATASAMUDAYA task schedulers and executors to
			// communicate
			// via task statuses.
			if (Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					&& Boolean.FALSE.equals(isjgroups) && !isignite) {
				// Initialize the heart beat for gathering the resources
				// Initialize the heart beat for gathering the task executors
				// task statuses information.
				if (job.getPipelineconfig().getIsremotescheduler()) {
					taskexecutors = new LinkedHashSet<>(job.getTaskexecutors());
				} else {
					getTaskExecutorsHostPort();
				}
				batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
				var taskexecutorssize = taskexecutors.size();
				taskexecutorssize = taskexecutors.size();
				log.debug("taskexecutorssize: " + taskexecutorssize);
				Utils.writeToOstream(pipelineconfig.getOutput(), "taskexecutorssize: " + taskexecutorssize);
				boolean haenabled = Boolean.parseBoolean(pipelineconfig.getTsshaenabled());
				if (!Objects.isNull(pipelineconfig.getJar()) && haenabled) {
					TssHAHostPorts.get().forEach(hp -> {
						try {
							LoadJar lj = new LoadJar();
							lj.setMrjar(pipelineconfig.getJar());
							String jarloaded = (String) Utils.getResultObjectByInput(hp, lj, job.getId());
							if (!Objects.isNull(jarloaded) && jarloaded.equals(DataSamudayaConstants.JARLOADED)) {
								log.info("Jar bundled to the server with host and port " + hp);
							} else {
								log.info("Jar not bundled to the server with host and port " + hp);
							}

						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					});
				}
				if (haenabled) {
					istaskcancelled.set(false);
					ping(job);
				}
			} else if (Boolean.TRUE.equals(isjgroups) && !isignite) {
				if (job.getPipelineconfig().getIsremotescheduler()) {
					taskexecutors = new LinkedHashSet<>(job.getTaskexecutors());
				} else {
					getTaskExecutorsHostPort();
				}
			}
			var uniquestagestoprocess = new ArrayList<>(job.getTopostages());
			var stagenumber = 0;
			graph = new SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>(DAGEdge.class);
			var taskgraph = new SimpleDirectedGraph<Task, DAGEdge>(DAGEdge.class);
			job.getJm().setTaskGraphs(taskgraph);
			// Generate Physical execution plan for each stages.
			if (Objects.isNull(job.getVertices())) {
				for (var stage : uniquestagestoprocess) {
					JobStage js = new JobStage();
					js.setJobid(job.getId());
					js.setStageid(stage.id);
					js.setStage(stage);
					jsidjsmap.put(job.getId() + stage.id, js);
					partitionindex = 0;
					var nextstage = stagenumber + 1 < uniquestagestoprocess.size()
							? uniquestagestoprocess.get(stagenumber + 1)
							: null;
					stage.number = stagenumber;
					generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(), graph,
							taskgraph);
					stagenumber++;
				}
				job.setVertices(new LinkedHashSet<>(graph.vertexSet()));
				job.setEdges(new LinkedHashSet<>(graph.edgeSet()));
			} else {
				job.getVertices().stream().forEach(vertex -> graph.addVertex((StreamPipelineTaskSubmitter) vertex));
				job.getEdges().stream().forEach(edge -> graph.addEdge((StreamPipelineTaskSubmitter) edge.getSource(),
						(StreamPipelineTaskSubmitter) edge.getTarget()));
			}
			batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
			semaphore = new Semaphore(batchsize);
			var writer = new StringWriter();
			if (Boolean
					.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
				Utils.renderGraphPhysicalExecPlan(taskgraph, writer);
			}
			// Topological ordering of the job stages in
			// MassiveDataStreamTaskSchedulerThread is processed.
			Iterator<StreamPipelineTaskSubmitter> topostages = new TopologicalOrderIterator(graph);
			var sptsl = new ArrayList<StreamPipelineTaskSubmitter>();
			// Sequential ordering of topological ordering is obtained to
			// process for parallelization.
			while (topostages.hasNext()) {
				sptsl.add(topostages.next());
			}
			log.debug("{}", sptsl);
			var sptss = getFinalPhasesWithNoSuccessors(graph, sptsl);
			var partitionnumber = 0;
			var ishdfs = false;
			if (nonNull(job.getUri())) {
				ishdfs = new URL(job.getUri()).getProtocol().equals(DataSamudayaConstants.HDFS_PROTOCOL);
			}
			for (var spts : sptss) {
				spts.getTask().finalphase = true;
				if (job.getTrigger() == job.getTrigger().SAVERESULTSTOFILE && ishdfs) {
					spts.getTask().saveresulttohdfs = true;
					spts.getTask().hdfsurl = job.getUri();
					spts.getTask().filepath = job.getSavepath() + DataSamudayaConstants.HYPHEN + partitionnumber++;
				}
			}
			if (isignite) {
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedCounter().inc();
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedIgniteModeCounter().inc();
				parallelExecutionPhaseIgnite(graph, new TaskProviderIgnite());
			} else if (Boolean.TRUE.equals(islocal)) {
				job.setResultstream(resultstream);
				batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
				semaphore = new Semaphore(batchsize);
				cache = DataSamudayaCache.get();
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedCounter().inc();
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedLocalModeCounter().inc();
				if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
					int akkaport = Utils.getRandomPort();
					Config config = Utils.getAkkaSystemConfig(
							DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOST),
							akkaport, Runtime.getRuntime().availableProcessors());
					system = ActorSystem.create(DataSamudayaConstants.ACTORUSERNAME, config);
					Cluster cluster = Cluster.get(system);
					cluster.joinSeedNodes(Arrays.asList(cluster.selfAddress()));
					final String actorsystemurl = DataSamudayaConstants.AKKA_URL_SCHEME + "://"
							+ DataSamudayaConstants.ACTORUSERNAME + "@"
							+ system.provider().getDefaultAddress().getHost().get() + ":"
							+ system.provider().getDefaultAddress().getPort().get() + "/user";
					parallelExecutionAkkaActorsLocal(system, actorsystemurl, graph);
				} else {
					parallelExecutionPhaseDExecutorLocalMode(graph,
							new TaskProviderLocalMode(graph.vertexSet().size()));
				}
			} else if (Boolean.TRUE.equals(ismesos)) {
				MesosScheduler.runFramework(sptsl, graph,
						DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MESOS_MASTER), tasksptsthread,
						pipelineconfig.getJar());
			} else if (Boolean.TRUE.equals(isyarn)) {
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedCounter().inc();
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedYarnModeCounter().inc();
				if (!pipelineconfig.getUseglobaltaskexecutors()) {
					yarnmutex.acquire();
					pipelineconfig.setJobid(job.getId());
					Utils.createJobInHDFS(pipelineconfig, sptsl, graph, tasksptsthread, jsidjsmap);
					decideContainerCountAndPhysicalMemoryByBlockSize(sptsl.size(),
							Integer.parseInt(pipelineconfig.getBlocksize()));
					ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
							DataSamudayaConstants.FORWARD_SLASH + YarnSystemConstants.DEFAULT_CONTEXT_FILE_CLIENT,
							getClass());
					var client = (CommandYarnClient) context.getBean(DataSamudayaConstants.YARN_CLIENT);
					if (nonNull(pipelineconfig.getJobname())) {
						client.setAppName(pipelineconfig.getJobname());
					} else {
						client.setAppName(DataSamudayaConstants.DATASAMUDAYA);
					}
					client.getEnvironment().put(DataSamudayaConstants.YARNDATASAMUDAYAJOBID, job.getId());
					var appid = client.submitApplication(true);
					var appreport = client.getApplicationReport(appid);
					yarnmutex.release();
					while (appreport.getYarnApplicationState() != YarnApplicationState.RUNNING) {
						appreport = client.getApplicationReport(appid);
						Thread.sleep(1000);
					}
					Utils.sendJobToYARNDistributedQueue(zo, job.getId());
					TaskInfoYARN tinfoyarn = Utils.getJobOutputStatusYARNDistributedQueueBlocking(zo, job.getId());
					Utils.shutDownYARNContainer(zo, job.getId());
					log.info("Request jobid {} matching Response job id {} is {}", job.getId(), tinfoyarn.getJobid(),
							job.getId().equals(tinfoyarn.getJobid()));
				} else {
					Utils.createJobInHDFS(pipelineconfig, sptsl, graph, tasksptsthread, jsidjsmap);
					Utils.sendJobToYARNDistributedQueue(pipelineconfig.getTejobid(), job.getId());
					TaskInfoYARN tinfoyarn = Utils
							.getJobOutputStatusYARNDistributedQueueBlocking(pipelineconfig.getTejobid());
					log.info("Request jobid {} matching Response job id {} is {}", job.getId(), tinfoyarn.getJobid(),
							job.getId().equals(tinfoyarn.getJobid()));
					log.info("Is output available {}", tinfoyarn.isIsresultavailable());
				}
			} else if (Boolean.TRUE.equals(isjgroups)) {
				Iterator<Task> toposort = new TopologicalOrderIterator(taskgraph);
				var tasks = new ArrayList<Task>();
				taskexecutors = new LinkedHashSet<>();
				while (toposort.hasNext()) {
					var task = toposort.next();
					taskexecutors.add(task.hostport);
					tasks.add(task);
				}
				tasksgraphexecutor = new TasksGraphExecutor[taskexecutors.size()];
				var taskgraphexecutormap = new ConcurrentHashMap<String, TasksGraphExecutor>();
				var stagegraphexecutorindex = 0;
				for (var te : taskexecutors) {
					tasksgraphexecutor[stagegraphexecutorindex] = new TasksGraphExecutor();
					tasksgraphexecutor[stagegraphexecutorindex].setTasks(new ArrayList<>());
					tasksgraphexecutor[stagegraphexecutorindex].setHostport(te);
					if (job.getJobtype() == JOBTYPE.PIG || nonNull(pipelineconfig.getStorage())
							&& (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL)) {
						tasksgraphexecutor[stagegraphexecutorindex].setStorage(pipelineconfig.getStorage());
					} else {
						tasksgraphexecutor[stagegraphexecutorindex].setStorage(STORAGE.DISK);
					}
					taskgraphexecutormap.put(te, tasksgraphexecutor[stagegraphexecutorindex]);
					stagegraphexecutorindex++;
				}
				stagegraphexecutorindex = 0;
				Task tasktmp = null;
				toposort = new TopologicalOrderIterator(taskgraph);
				// Sequential ordering of topological ordering is obtained to
				// process for parallelization.
				tasks.clear();
				while (toposort.hasNext()) {
					tasktmp = toposort.next();
					tasks.add(tasktmp);
					var taskspredecessor = Graphs.predecessorListOf(taskgraph, tasktmp);
					tasktmp.taskspredecessor = taskspredecessor;
					var taskgraphexecutor = taskgraphexecutormap.get(tasktmp.hostport);
					taskgraphexecutor.getTasks().add(tasktmp);
				}
				log.debug("{}", Arrays.asList(tasksgraphexecutor));
				String jobid = job.getId();
				if (pipelineconfig.getUseglobaltaskexecutors()) {
					jobid = pipelineconfig.getTejobid();
				}
				broadcastJobStageToTaskExecutors(tasks);
				for (var stagesgraphexecutor : tasksgraphexecutor) {
					if (!stagesgraphexecutor.getTasks().isEmpty()) {
						Utils.getResultObjectByInput(stagesgraphexecutor.getHostport(), stagesgraphexecutor, jobid);
					}
				}
				var stagepartids = tasks.parallelStream().map(taskpart -> taskpart.jobid + taskpart.taskid)
						.collect(Collectors.toSet());
				var stagepartidstatusmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				var stagepartidstatusmapreq = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
				try (var channel = Utils.getChannelTaskExecutor(jobid,
						NetworkUtil.getNetworkAddress(DataSamudayaProperties.get()
								.getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST)),
						Integer.parseInt(DataSamudayaProperties.get()
								.getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT)),
						stagepartidstatusmapreq, stagepartidstatusmapresp);) {
					var totaltasks = tasks.size();
					DataSamudayaMetricsExporter.getNumberOfJobSubmittedCounter().inc();
					DataSamudayaMetricsExporter.getNumberOfJobSubmittedJgroupsModeCounter().inc();
					while (true) {
						Utils.whoare(channel);
						var totalcompleted = 0.0;
						for (var stagepart : stagepartids) {
							if (stagepartidstatusmapresp.get(stagepart) == WhoIsResponse.STATUS.COMPLETED) {
								totalcompleted++;
							}
						}
						double percentagecompleted = Math.floor((totalcompleted / totaltasks) * 100.0);
						if (totalcompleted == totaltasks) {
							Utils.writeToOstream(pipelineconfig.getOutput(),
									"\nPercentage Completed " + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put("", percentagecompleted);
							sptsl.parallelStream().forEach(spts -> spts.setCompletedexecution(true));
							break;
						} else {
							log.debug(
									"Total Percentage Completed: " + Math.floor((totalcompleted / totaltasks) * 100.0));
							Utils.writeToOstream(pipelineconfig.getOutput(),
									"\nPercentage Completed " + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put("", percentagecompleted);
							Thread.sleep(4000);
						}
					}
					stagepartidstatusmapresp.clear();
					stagepartidstatusmapreq.clear();
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			} else {
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedCounter().inc();
				DataSamudayaMetricsExporter.getNumberOfJobSubmittedStandaloneModeCounter().inc();
				broadcastJobStageToTaskExecutors(new ArrayList<>(taskgraph.vertexSet()));
				if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
					graph = (SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>) parallelExecutionAkkaActors(
							graph);
				} else {
					graph = (SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>) parallelExecutionPhaseDExecutor(
							graph);
				}
			}

			// Obtain the final stage job results after final stage is
			// completed.
			List finalstageoutput = new ArrayList<>();
			if (job.getTrigger() == TRIGGER.FOREACH) {
				finalstageoutput = new ArrayList<>(sptss);
			} else {
				finalstageoutput = getLastStageOutput(sptss, graph, sptsl, ismesos, isyarn, islocal, isjgroups,
						resultstream, taskgraph);
			}
			if (Boolean.TRUE.equals(isjgroups) && job.getJobtype() != JOBTYPE.PIG) {
				closeResourcesTaskExecutor(tasksgraphexecutor);
			}
			printStats();
			return finalstageoutput;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
			return null;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERERROR, ex);
		} finally {
			if (!Objects.isNull(job.getIgcache()) && job.getTrigger() != TRIGGER.FOREACH) {
				job.getIgcache().close();
			}
			if ((Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn) && Boolean.FALSE.equals(islocal)
					|| Boolean.TRUE.equals(isjgroups)) && !isignite) {
				if (!pipelineconfig.getUseglobaltaskexecutors()) {
					if (!Boolean.TRUE.equals(isjgroups)) {
						var cce = new FreeResourcesCompletedJob();
						cce.setJobid(job.getId());
						var tes = zo.getTaskExectorsByJobId(job.getId());
						for (var te : tes) {
							Utils.getResultObjectByInput(te, cce, job.getId());
						}
					}
					zo.deleteJob(job.getId());
					if (job.getTrigger() != TRIGGER.FOREACH && !job.getPipelineconfig().getIsremotescheduler()) {
						Utils.destroyTaskExecutors(job);
					}
				}
			}
			if (zo != null) {
				zo.close();
			}
			if (!Objects.isNull(job.getAllstageshostport())) {
				job.getAllstageshostport().clear();
			}
			if (!Objects.isNull(job.getStageoutputmap())) {
				job.getStageoutputmap().clear();
			}
			istaskcancelled.set(true);
			jobping.shutdownNow();
			if (nonNull(system)) {
				system.terminate();
				Utils.cleanupTaskActorFromSystem(system, actorrefs, job.getId());
			}
			if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL && CollectionUtils.isNotEmpty(taskexecutors)) {
				for(String tehost:taskexecutors) {
					Utils.getResultObjectByInput(tehost, new CleanupTaskActors(job.getId()), pipelineconfig.getTejobid());
				}
			}
		}

	}

	/**
	 * Print Status of job
	 * 
	 * @throws Exception
	 */
	protected void printStats() throws Exception {
		job.setIscompleted(true);
		job.getJm().setJobcompletiontime(System.currentTimeMillis());
		Utils.writeToOstream(pipelineconfig.getOutput(), "\nConcluded job in "
				+ ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
		log.info("Concluded job in " + ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0)
				+ " seconds");
		job.getJm().setTotaltimetaken((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0);
	}

	/**
	 * Get final stages which has no successors.
	 * 
	 * @param graph
	 * @param sptsl
	 * @return
	 */
	public Set<Task> getFinalPhasesWithNoSuccessors(SimpleDirectedGraph<Task, DAGEdge> taskgraph) {
		return taskgraph.vertexSet().stream().filter(task -> Graphs.successorListOf(taskgraph, task).isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Broadcast Rrequired JS to executors
	 * 
	 * @param tasks
	 * @throws Exception
	 */
	public void broadcastJobStageToTaskExecutors(List<Task> tasks) throws Exception {
		Kryo kryo = Utils.getKryoInstance();
		if (nonNull(pipelineconfig.getClsloader())) {
			kryo.setClassLoader(pipelineconfig.getClsloader());
		}
		String jobid = job.getId();
		if (pipelineconfig.getUseglobaltaskexecutors()) {
			jobid = pipelineconfig.getTejobid();
		}
		final String finaljobid = jobid;
		Map<String, Set<String>> jobexecutorsmap = tasks.stream()
				.collect(Collectors.groupingBy(task -> task.jobid + task.stageid, HashMap::new,
						Collectors.mapping(task -> task.hostport, Collectors.toSet())));
		jobexecutorsmap.keySet().stream().forEach(key -> {
			try {
				JobStage js = (JobStage) jsidjsmap.get(key);
				if (nonNull(js)) {
					js.setTejobid(finaljobid);				
					for (String te : jobexecutorsmap.get(key)) {
						if(nonNull(job.getPipelineconfig().getJar())) {
							Utils.getResultObjectByInput(te, js, finaljobid, DataSamudayaMapReducePhaseClassLoader.newInstance(job.getPipelineconfig().getJar(), Thread.currentThread().getContextClassLoader()));
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

	/**
	 * Get containers host port by launching.
	 *
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public void getTaskExecutorsHostPort() throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			var loadjar = new LoadJar();
			loadjar.setMrjar(pipelineconfig.getJar());
			if (nonNull(pipelineconfig.getCustomclasses()) && !pipelineconfig.getCustomclasses().isEmpty()) {
				loadjar.setClasses(pipelineconfig.getCustomclasses().stream().map(clz -> clz.getName())
						.collect(Collectors.toCollection(LinkedHashSet::new)));
			}
			Map<String, List<Integer>> nodehostportteports = new ConcurrentHashMap<>();
			for (var lc : job.getLcs()) {
				List<Integer> ports = null;
				if (pipelineconfig.getUseglobaltaskexecutors()) {
					ports = lc.getCla().getCr().stream().map(cr -> {
						return cr.getPort();
					}).collect(Collectors.toList());
				} else {
					ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc,
							DataSamudayaConstants.EMPTY);
				}
				if (CollectionUtils.isNotEmpty(nodehostportteports.get(lc.getNodehostport()))) {
					nodehostportteports.get(lc.getNodehostport()).addAll(ports);
				} else {
					nodehostportteports.put(lc.getNodehostport(), ports);
				}
			}
			int sotimeoutcountmax = 100;
			int sotimeoutcount = 0;
			for (Entry<String, List<Integer>> nodehostportentry : nodehostportteports.entrySet()) {
				String nodehostport = nodehostportentry.getKey();
				String tehost = nodehostport.split("_")[0];
				List<Integer> ports = nodehostportentry.getValue();
				for (int port : ports) {
					sotimeoutcount = 0;
					while (sotimeoutcount < sotimeoutcountmax) {
						try (Socket sock = new Socket()) {
							sock.connect(new InetSocketAddress(tehost, port), 2000);
							break;
						} catch (Exception ex) {
							Thread.sleep(200);
						}
						sotimeoutcount++;
					}
					if (nonNull(loadjar.getMrjar())) {
						log.debug("{}",
								Utils.getResultObjectByInput(tehost + DataSamudayaConstants.UNDERSCORE + port, loadjar,
										pipelineconfig.getUseglobaltaskexecutors() ? pipelineconfig.getTejobid()
												: job.getId()));
					}
				}
			}
			String jobid = job.getId();
			if (pipelineconfig.getUseglobaltaskexecutors()) {
				jobid = pipelineconfig.getTejobid();
			}
			var tes = zo.getTaskExectorsByJobId(jobid);
			taskexecutors = new LinkedHashSet<>(tes);
			if (taskexecutors.size() != job.getTaskexecutors().size()) {
				log.warn("Current Task Executors {} is not same as predetermined taskexecutors for current job {}",
						taskexecutors, job.getTaskexecutors());
			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Closes Stages in task executor making room for another tasks to be
	 * executed in task executors JVM.
	 * 
	 * @param tasksgraphexecutor
	 * @throws Exception
	 */
	private void closeResourcesTaskExecutor(TasksGraphExecutor[] tasksgraphexecutor) throws Exception {
		String jobid = job.getId();
		if (pipelineconfig.getUseglobaltaskexecutors()) {
			jobid = pipelineconfig.getTejobid();
		}
		for (var taskgraphexecutor : tasksgraphexecutor) {
			if (!taskgraphexecutor.getTasks().isEmpty()) {
				var hp = taskgraphexecutor.getHostport();
				Task task = (Task) Utils.getResultObjectByInput(hp,
						new CloseStagesGraphExecutor(taskgraphexecutor.getTasks()), jobid);
				if (nonNull(task)) {
					log.info("Wrap up of tasks completed successfully for host " + hp);
				}
			}
		}
	}

	/**
	 * Creates DExecutor object and executes tasks.
	 * 
	 * @param graph
	 * @param taskprovider
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void parallelExecutionPhaseDExecutorLocalMode(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			TaskProvider taskprovider) throws Exception {
		var es = newExecutor(batchsize);
		try {

			var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es, taskprovider);
			var executor = new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(config);
			var edges = graph.edgeSet();
			if (!edges.isEmpty()) {
				for (var edge : edges) {
					executor.addDependency((StreamPipelineTaskSubmitter) edge.getSource(),
							(StreamPipelineTaskSubmitter) edge.getTarget());
				}
			} else {
				var vertices = graph.vertexSet();
				for (var vertex : vertices)
					executor.addDependency(vertex, vertex);
			}
			executor.execute(ExecutionConfig.NON_TERMINATING);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}

	/**
	 * Creates DExecutor object and executes tasks in ignite server.
	 * 
	 * @param graph
	 * @param taskprovider
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void parallelExecutionPhaseIgnite(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			TaskProvider taskprovider) throws Exception {
		ExecutorService es = null;
		try {
			es = newExecutor(batchsize);
			var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es, taskprovider);
			var executor = new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(config);
			var edges = graph.edgeSet();
			if (!edges.isEmpty()) {
				for (var edge : edges) {
					executor.addDependency((StreamPipelineTaskSubmitter) edge.getSource(),
							(StreamPipelineTaskSubmitter) edge.getTarget());
				}
			} else {
				var vertices = graph.vertexSet();
				for (var vertex : vertices)
					executor.addDependency(vertex, vertex);
			}
			executor.execute(ExecutionConfig.NON_TERMINATING);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULEPARALLELEXECUTIONERROR, ex);
		} finally {
			if (!Objects.isNull(es)) {
				es.shutdownNow();
				es.awaitTermination(1, TimeUnit.SECONDS);
			}
		}
	}

	Map<String, Integer> servertotaltasks = new ConcurrentHashMap<>();
	Map<String, Double> tetotaltaskscompleted = new ConcurrentHashMap<>();
	Map<String, Double> tetotaltasksfailed = new ConcurrentHashMap<>();

	/**
	 * Creates DExecutor object, executes tasks and reexecutes tasks if fails.
	 * 
	 * @param graph
	 * @throws Exception
	 */
	public Graph<StreamPipelineTaskSubmitter, DAGEdge> parallelExecutionPhaseDExecutor(
			final Graph<StreamPipelineTaskSubmitter, DAGEdge> origgraph) throws Exception {
		ExecutorService es = null;
		var lineagegraph = origgraph;
		try {
			var completed = false;
			var numexecute = 0;
			var executioncount = Integer.parseInt(pipelineconfig.getExecutioncount());
			batchsize = 0;
			List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> erroredresult = null;

			var temdstdtmap = new ConcurrentHashMap<String, List<StreamPipelineTaskSubmitter>>();
			var chpcres = GlobalContainerAllocDealloc.getHportcrs();
			if (chpcres.isEmpty()) {
				job.getLcs().stream().forEach(lcs -> {
					String host = lcs.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
					lcs.getCla().getCr().stream()
							.forEach(cr -> chpcres.put(host + DataSamudayaConstants.UNDERSCORE + cr.getPort(), cr));
				});
			}
			var semaphores = new ConcurrentHashMap<String, Semaphore>();
			for (var cr : chpcres.entrySet()) {
				batchsize += cr.getValue().getCpu();
				semaphores.put(cr.getKey(), new Semaphore(cr.getValue().getCpu()));
			}
			es = newExecutor(batchsize);

			while (!completed && numexecute < executioncount) {
				temdstdtmap.clear();
				var shouldcontinueprocessing = new AtomicBoolean(true);
				var configexec = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(es,
						new DAGScheduler(lineagegraph.vertexSet().size(), semaphores, shouldcontinueprocessing));
				var dexecutor = new DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>(configexec);

				var vertices = lineagegraph.vertexSet();
				for (var spts : vertices) {
					var predecessors = Graphs.predecessorListOf(lineagegraph, spts);
					if (predecessors.size() > 0) {
						for (var pred : predecessors) {
							dexecutor.addDependency(pred, spts);
							log.debug(pred + "->" + spts);
						}
					} else {
						dexecutor.addDependency(spts, spts);
					}
					if (Objects.isNull(servertotaltasks.get(spts.getHostPort()))) {
						servertotaltasks.put(spts.getHostPort(), 1);
					} else {
						servertotaltasks.put(spts.getHostPort(), servertotaltasks.get(spts.getHostPort()) + 1);
					}
				}
				var executionresultscomplete = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
				erroredresult = executionresultscomplete.getErrored();
				if (erroredresult.isEmpty() && !isErrored(lineagegraph.vertexSet())) {
					completed = true;
				} else {
					List<String> currentcontainers = null;
					if (pipelineconfig.getUseglobaltaskexecutors()) {
						currentcontainers = zo.getTaskExectorsByJobId(pipelineconfig.getTejobid());
					} else {
						currentcontainers = zo.getTaskExectorsByJobId(job.getId());
					}
					var containersfailed = job.getTaskexecutors();
					containersfailed.removeAll(currentcontainers);
					updateOriginalGraph(lineagegraph, containersfailed, currentcontainers);
					tetotaltaskscompleted.clear();
					servertotaltasks.clear();
					broadcastJobStageToTaskExecutors(
							lineagegraph.vertexSet().stream().map(spts -> spts.getTask()).collect(Collectors.toList()));
					log.debug("Tasks Errored: Original Graph {} \nGenerating Lineage Graph: {}", origgraph,
							lineagegraph);
				}

				numexecute++;
			}
			Utils.writeToOstream(pipelineconfig.getOutput(), "Number of Executions: " + numexecute);
			if (!completed) {
				StringBuilder sb = new StringBuilder();
				if (erroredresult != null) {
					erroredresult.forEach(exec -> {
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append(exec.getId().getTask().stagefailuremessage);
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
		}
		return lineagegraph;
	}

	/**
	 * The method executes after converting the tasks to actors in local mode
	 * 
	 * @param origgraph
	 * @return graph object lineage
	 * @throws Exception
	 */
	public Graph<StreamPipelineTaskSubmitter, DAGEdge> parallelExecutionAkkaActorsLocal(final ActorSystem system,
			String actorsystemurl, final Graph<StreamPipelineTaskSubmitter, DAGEdge> origgraph) throws Exception {
		ExecutorService es = null;
		ExecutorService esroot = null;
		var lineagegraph = origgraph;
		try {
			var completed = false;
			var numexecute = 0;
			var executioncount = Integer.parseInt(pipelineconfig.getExecutioncount());
			List<ExecutionResult<DefaultDexecutor, ExecutionResults<StreamPipelineTaskSubmitter, Boolean>>> successresult = null;
			es = newExecutor(Runtime.getRuntime().availableProcessors());
			ExecutionResults<StreamPipelineTaskSubmitter, Boolean> executionresultscomplete = null;
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
					jobid = job.getId();
				}
				List<StreamPipelineTaskSubmitter> sptss = new ArrayList<>();
				while (iterator.hasNext()) {
					StreamPipelineTaskSubmitter sptsreverse = iterator.next();
					var predecessors = Graphs.predecessorListOf(graphreversed, sptsreverse);
					var successors = Graphs.successorListOf(graphreversed, sptsreverse);
					if (CollectionUtils.isEmpty(predecessors)) {
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), null, successors.size());
						Task task = (Task) SQLUtils.getAkkaActor(system, gettaskactor, jsidjsmap, hdfs, cache,
								jobidstageidtaskidcompletedmap, actorsystemurl, Cluster.get(system), null, actorrefs);
						sptsreverse.getTask().setActorselection(task.getActorselection());
					} else {
						var childactorsoriggraph = predecessors.stream().map(spts -> spts.getTask().getActorselection())
								.collect(Collectors.toList());
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), childactorsoriggraph,
								successors.size());
						Task task = (Task) SQLUtils.getAkkaActor(system, gettaskactor, jsidjsmap, hdfs, cache,
								jobidstageidtaskidcompletedmap, actorsystemurl, Cluster.get(system), null, actorrefs);
						sptsreverse.getTask().setActorselection(task.getActorselection());
						sptsreverse.setChildactors(childactorsoriggraph);
					}
					if (CollectionUtils.isEmpty(successors)) {
						sptss.add(sptsreverse);
					}
				}
				var configexec = new DexecutorConfig<StreamPipelineTaskSubmitter, Boolean>(es,
						new TaskProviderLocalModeAkkaActors(Double.valueOf("" + sptss.size()), system, actorsystemurl));
				var dexecutor = new DefaultDexecutor<StreamPipelineTaskSubmitter, Boolean>(configexec);
				sptss.stream().forEach(dexecutor::addIndependent);
				executionresultscomplete = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
				successresult = (List) executionresultscomplete.getSuccess();
				if (sptss.size() == successresult.size()) {
					completed = true;
				} else {
					throw new PipelineException("Error In Executing Tasks");
				}
				numexecute++;
			}
			Utils.writeToOstream(pipelineconfig.getOutput(), "Number of Executions: " + numexecute);
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
	 * This class is task provider for akka actors executed in local mode
	 * 
	 * @author arun
	 *
	 */
	public class TaskProviderLocalModeAkkaActors implements TaskProvider<StreamPipelineTaskSubmitter, Boolean> {

		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Semaphore printresults = new Semaphore(1);
		ActorSystem system;
		String actorsystemurl;

		public TaskProviderLocalModeAkkaActors(double totaltasks, ActorSystem system, String actorsystemurl) {
			this.totaltasks = totaltasks;
			this.system = system;
			this.actorsystemurl = actorsystemurl;
		}

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean> provideTask(
				final StreamPipelineTaskSubmitter spts) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, Boolean>() {
				Task task = spts.getTask();
				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
						semaphore.acquire();
						int filepartitionstartindex = 0;
						List<String> actorsselection;
						if (CollectionUtils.isNotEmpty(task.getShufflechildactors())) {
							actorsselection = task.getShufflechildactors().stream().map(task -> task.actorselection)
									.collect(Collectors.toList());
						} else {
							actorsselection = spts.getChildactors();
						}
						ExecuteTaskActor eta = new ExecuteTaskActor(task, actorsselection, filepartitionstartindex);
						Task task = SQLUtils.getAkkaActor(system, eta, jsidjsmap, hdfs, cache,
								jobidstageidtaskidcompletedmap, actorsystemurl, Cluster.get(system), null, actorrefs);
						Utils.writeToOstream(pipelineconfig.getOutput(),
								"Completed Job And Stages: " + spts.getTask().jobid + DataSamudayaConstants.HYPHEN
										+ spts.getTask().stageid + DataSamudayaConstants.HYPHEN + spts.getTask().taskid
										+ " in " + task.timetakenseconds + " seconds");
						semaphore.release();
						printresults.acquire();
						counttaskscomp++;
						Utils.writeToOstream(pipelineconfig.getOutput(),
								"\nPercentage Completed " + Math.floor((counttaskscomp / totaltasks) * 100.0) + "% \n");
						printresults.release();
						return true;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("SleepyTaskProvider error", e);
					}

					return false;
				}
			};
		}
	}

	/**
	 * The method executes after converting the tasks to actors in the task
	 * executors
	 * 
	 * @param origgraph
	 * @return graph object lineage
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
			var executioncount = Integer.parseInt(pipelineconfig.getExecutioncount());
			List<ExecutionResult<StreamPipelineTaskSubmitter, Boolean>> successresult = null;
			es = newExecutor(Runtime.getRuntime().availableProcessors());
			ExecutionResults<StreamPipelineTaskSubmitter, Boolean> executionresultscomplete = null;
			int batchsize = 0;

			var temdstdtmap = new ConcurrentHashMap<String, List<StreamPipelineTaskSubmitter>>();
			var chpcres = GlobalContainerAllocDealloc.getHportcrs();
			if (chpcres.isEmpty()) {
				job.getLcs().stream().forEach(lcs -> {
					String host = lcs.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
					lcs.getCla().getCr().stream()
							.forEach(cr -> chpcres.put(host + DataSamudayaConstants.UNDERSCORE + cr.getPort(), cr));
				});
			}
			var semaphores = new ConcurrentHashMap<String, Semaphore>();
			for (var cr : chpcres.entrySet()) {
				batchsize += cr.getValue().getCpu();
				semaphores.put(cr.getKey(), new Semaphore(cr.getValue().getCpu()));
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
					jobid = job.getId();
				}
				List<StreamPipelineTaskSubmitter> sptss = new ArrayList<>();
				while (iterator.hasNext()) {
					StreamPipelineTaskSubmitter sptsreverse = iterator.next();
					var predecessors = Graphs.predecessorListOf(graphreversed, sptsreverse);
					var successors = Graphs.successorListOf(graphreversed, sptsreverse);
					if (CollectionUtils.isEmpty(predecessors)) {
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), null, successors.size());
						Task task = (Task) Utils.getResultObjectByInput(sptsreverse.getHostPort(), gettaskactor, jobid);
						sptsreverse.getTask().setActorselection(task.getActorselection());
					} else {
						var childactorsoriggraph = predecessors.stream().map(spts -> spts.getTask().getActorselection())
								.collect(Collectors.toList());
						GetTaskActor gettaskactor = new GetTaskActor(sptsreverse.getTask(), childactorsoriggraph,
								successors.size());
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
			Utils.writeToOstream(pipelineconfig.getOutput(), "Number of Executions: " + numexecute);
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
	 * The Scheduler for Akka Actors Task
	 * 
	 * @author arun
	 *
	 */
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
							spts.setTaskexecutors(job.getTaskexecutors());
							Task result = (Task) spts.actors();
							log.info("Task Status for task {} is {}", result.getTaskid(), result.taskstatus);
							printresult.acquire();
							counttaskscomp++;
							double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
							Utils.writeToOstream(pipelineconfig.getOutput(),
									"\nPercentage Completed " + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put(spts.getHostPort(), percentagecompleted);
							if (Objects.isNull(job.getJm().getTaskexcutortasks().get(spts.getTask().getHostport()))) {
								job.getJm().getTaskexcutortasks().put(spts.getTask().getHostport(), new ArrayList<>());
							}
							printresult.release();
							if (result.taskstatus == TaskStatus.FAILED) {
								spts.getTask().setTaskstatus(TaskStatus.FAILED);
								spts.getTask().stagefailuremessage = result.stagefailuremessage;
								spts.setCompletedexecution(false);
								shouldcontinueprocessing.set(false);
								throw new IllegalArgumentException("Task Failed");
							} else if (result.taskstatus == TaskStatus.COMPLETED) {
								spts.setCompletedexecution(true);
								job.getJm().getTaskexcutortasks().get(spts.getTask().getHostport()).add(result);
							}
							spts.getTask().setPiguuid(result.getPiguuid());
							return true;
						} else if (spts.isCompletedexecution() && shouldcontinueprocessing.get()) {
							printresult.acquire();
							double percentagecompleted = Math.floor((counttaskscomp / totaltasks) * 100.0);
							Utils.writeToOstream(pipelineconfig.getOutput(),
									"\nPercentage Completed" + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put(spts.getHostPort(), percentagecompleted);
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

	public boolean isErrored(Set<StreamPipelineTaskSubmitter> spts) {
		Optional<StreamPipelineTaskSubmitter> optionalspts = spts.stream().parallel()
				.filter(spt -> !spt.isCompletedexecution()).findFirst();
		if (optionalspts.isPresent()) {
			return true;
		}
		return false;
	}

	/**
	 * Creates Executors
	 *
	 * @return ExecutorsService object.
	 */
	private ExecutorService newExecutor(int numberoftasks) {
		return Executors.newFixedThreadPool(numberoftasks);
	}

	/**
	 * The method generates lineage graph from original graph.
	 * 
	 * @param graphToProcess
	 * @param downNodeIPs
	 * @param availableNodes
	 * @return lineage graph
	 */
	public void updateOriginalGraph(Graph<StreamPipelineTaskSubmitter, DAGEdge> graphtoprocess, List<String> downteips,
			List<String> availabletes) {
		List<StreamPipelineTaskSubmitter> failedtasks = getFailedTasks(graphtoprocess, downteips);
		updateGraph(graphtoprocess, failedtasks, availabletes, downteips);
	}

	/**
	 * Update the Tasks with the available taskexecutors
	 * 
	 * @param graph
	 * @param failedtasks
	 * @param availableteips
	 * @param downteips
	 */
	protected void updateGraph(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			List<StreamPipelineTaskSubmitter> failedtasks, List<String> availableteips, List<String> downteips) {
		// Iterate through all failed nodes
		for (StreamPipelineTaskSubmitter failedtask : failedtasks) {
			// Update the graph for each failed node
			updateChildren(graph, failedtask, getAvailableTaskExecutor(failedtask.getTask(), availableteips),
					downteips);
		}
	}

	/**
	 * Update children with the taskexecutors of parents.
	 * 
	 * @param graph
	 * @param parentNode
	 */
	protected void updateChildren(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			StreamPipelineTaskSubmitter currenttask, String hostporttoupdate, List<String> downnodeips) {

		if (downnodeips.contains(currenttask.getHostPort())) {
			// Update the child node with the parent's IP address and port
			currenttask.setHostPort(hostporttoupdate);
			currenttask.getTask().setHostport(hostporttoupdate);
			currenttask.setCompletedexecution(false);
		} else {
			var incomingedges = graph.incomingEdgesOf(currenttask);
			for (DAGEdge dagedge : incomingedges) {
				reConfigureParentRDFForStageExecution(currenttask, graph.getEdgeSource(dagedge));
			}
			updateChildrenTaskIncompleted(graph, currenttask);
			return;
		}

		for (DAGEdge edge : graph.outgoingEdgesOf(currenttask)) {
			StreamPipelineTaskSubmitter childtask = graph.getEdgeTarget(edge);
			// Recursively update the child's children
			updateChildren(graph, childtask, hostporttoupdate, downnodeips);
		}
	}

	/**
	 * Update Children When Task Incompleted
	 * 
	 * @param graph
	 * @param parenttask
	 */
	public void updateChildrenTaskIncompleted(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			StreamPipelineTaskSubmitter parenttask) {
		parenttask.setCompletedexecution(false);
		for (DAGEdge edge : graph.outgoingEdgesOf(parenttask)) {
			StreamPipelineTaskSubmitter childtask = graph.getEdgeTarget(edge);
			// Recursively update the child's children
			updateChildrenTaskIncompleted(graph, childtask);
		}
	}

	/**
	 * Get Failed Tasks based on Dead Task Executor
	 * 
	 * @param graph
	 * @param downNodeIps
	 * @return all the tasks that are failed
	 */
	protected List<StreamPipelineTaskSubmitter> getFailedTasks(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			List<String> downnodeips) {
		List<StreamPipelineTaskSubmitter> failedNodes = new ArrayList<>();
		graph.vertexSet().stream().filter(spts -> downnodeips.contains(spts.getHostPort())).forEach(failedNodes::add);
		return failedNodes;
	}

	Random rand = new Random(System.currentTimeMillis());

	public String getAvailableTaskExecutor(Task task, List<String> availableNodes) {
		Object[] inputs = task.getInput();
		if (nonNull(inputs)) {
			for (Object obj : inputs) {
				if (obj instanceof BlocksLocation bl) {
					Block[] bls = bl.getBlock();
					Map<String, Set<String>> dnxref = bls[0].getDnxref();
					Optional<String> dnxrefopt = availableNodes.stream().parallel()
							.map(node -> node.split(DataSamudayaConstants.UNDERSCORE)[0]).map(ip -> dnxref.get(ip))
							.filter(dnaddrs -> nonNull(dnaddrs)).flatMap(dnaddrs -> dnaddrs.stream()).findFirst();
					if (dnxrefopt.isPresent()) {
						String dn = dnxrefopt.get();
						bls[0].setHp(dn);
						Optional<String> availablenode = availableNodes.stream().parallel()
								.filter(node -> dn.startsWith(node.split(DataSamudayaConstants.UNDERSCORE)[0]))
								.findFirst();
						if (availablenode.isPresent()) {
							return availablenode.get();
						} else {
							return availableNodes.get(rand.nextInt(availableNodes.size()));
						}
					} else {
						return availableNodes.get(rand.nextInt(availableNodes.size()));
					}
				}
			}
		}
		return availableNodes.get(rand.nextInt(availableNodes.size()));
	}

	public void reConfigureParentRDFForStageExecution(StreamPipelineTaskSubmitter spts,
			StreamPipelineTaskSubmitter pred) {
		var prdf = spts.getTask().parentremotedatafetch;
		if (nonNull(prdf)) {
			for (var rdf : prdf) {
				if (!Objects.isNull(rdf) && rdf.getTaskid().equals(pred.getTask().getTaskid())) {
					rdf.setHp(pred.getHostPort());
					break;
				}
			}
		}
	}

	/**
	 * 
	 * @author arun The task provider for the local mode stage execution.
	 */
	public class TaskProviderLocalMode
			implements TaskProvider<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal> {

		ExecutorService es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Semaphore printresults = new Semaphore(1);

		public TaskProviderLocalMode(double totaltasks) {
			this.totaltasks = totaltasks;
		}

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal> provideTask(
				final StreamPipelineTaskSubmitter spts) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorLocal>() {
				Task task = spts.getTask();
				private static final long serialVersionUID = 1L;

				public StreamPipelineTaskExecutorLocal execute() {
					try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
						semaphore.acquire();
						StreamPipelineTaskExecutorLocal sptel = null;
						sptel = new StreamPipelineTaskExecutorLocal(jsidjsmap.get(task.jobid + task.stageid),
								resultstream, cache);
						sptel.setTask(task);
						sptel.setExecutor(jobping);
						sptel.setHdfs(hdfs);
						Future fut = es.submit(sptel);
						fut.get();
						Utils.writeToOstream(pipelineconfig.getOutput(),
								"Completed Job And Stages: " + spts.getTask().jobid + DataSamudayaConstants.HYPHEN
										+ spts.getTask().stageid + DataSamudayaConstants.HYPHEN + spts.getTask().taskid
										+ " in " + sptel.timetaken + " seconds");
						semaphore.release();
						printresults.acquire();
						counttaskscomp++;
						Utils.writeToOstream(pipelineconfig.getOutput(),
								"\nPercentage Completed " + Math.floor((counttaskscomp / totaltasks) * 100.0) + "% \n");
						printresults.release();
						return sptel;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("SleepyTaskProvider error", e);
					}

					return null;
				}
			};
		}
	}

	/**
	 * 
	 * @author arun The task provider for the ignite mode stage execution.
	 */
	public class TaskProviderIgnite
			implements TaskProvider<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite> {

		public com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite> provideTask(
				final StreamPipelineTaskSubmitter spts) {

			return new com.github.dexecutor.core.task.Task<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutorIgnite>() {

				private static final long serialVersionUID = 1L;

				public StreamPipelineTaskExecutorIgnite execute() {
					var task = spts.getTask();
					StreamPipelineTaskExecutorIgnite mdste = null;
					try {
						Kryo kryo = Utils.getKryoInstance();
						ByteArrayOutputStream baos = new ByteArrayOutputStream();
						Output out = new Output(baos);
						kryo.writeClassAndObject(out, jsidjsmap.get(task.jobid + task.stageid));
						out.flush();
						if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
							mdste = new StreamPipelineTaskExecutorIgniteSQL(baos.toByteArray(), task,
									pipelineconfig.isTopersistcolumnar());
						} else {
							mdste = new StreamPipelineTaskExecutorIgnite(baos.toByteArray(), task);
						}
						mdste.setHdfspath(hdfsfilepath);
						semaphore.acquire();
						var compute = job.getIgnite().compute(job.getIgnite().cluster().forServers());
						compute.affinityRun(DataSamudayaConstants.DATASAMUDAYACACHE, task.input[0], mdste);
						semaphore.release();
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("TaskProviderIgnite error", e);
					}
					return mdste;
				}
			};
		}
	}

	/**
	 * 
	 * @author arun The task provider for the standlone mode stage execution.
	 */
	public class DAGScheduler implements TaskProvider<StreamPipelineTaskSubmitter, Boolean> {
		Logger log = LoggerFactory.getLogger(DAGScheduler.class);
		double totaltasks;
		double counttaskscomp = 0;
		double counttasksfailed = 0;
		Map<String, Semaphore> semaphores;
		AtomicBoolean shouldcontinueprocessing;

		public DAGScheduler(double totaltasks, Map<String, Semaphore> semaphores,
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
							Task result = (Task) spts.call();
							log.info("Task Status for task {} is {}", result.getTaskid(), result.taskstatus);
							printresult.acquire();
							if (Objects.isNull(tetotaltaskscompleted.get(spts.getHostPort()))) {
								tetotaltaskscompleted.put(spts.getHostPort(), 0d);
							}
							counttaskscomp++;
							tetotaltaskscompleted.put(spts.getHostPort(),
									tetotaltaskscompleted.get(spts.getHostPort()) + 1);
							double percentagecompleted = Math.floor((tetotaltaskscompleted.get(spts.getHostPort())
									/ servertotaltasks.get(spts.getHostPort())) * 100.0);
							Utils.writeToOstream(pipelineconfig.getOutput(), "\nPercentage Completed TE("
									+ spts.getHostPort() + ") " + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put(spts.getHostPort(), percentagecompleted);
							if (Objects.isNull(job.getJm().getTaskexcutortasks().get(spts.getTask().getHostport()))) {
								job.getJm().getTaskexcutortasks().put(spts.getTask().getHostport(), new ArrayList<>());
							}
							printresult.release();
							if (result.taskstatus == TaskStatus.FAILED) {
								spts.getTask().setTaskstatus(TaskStatus.FAILED);
								spts.getTask().stagefailuremessage = result.stagefailuremessage;
								spts.setCompletedexecution(false);
								shouldcontinueprocessing.set(false);
								throw new IllegalArgumentException("Task Failed");
							} else if (result.taskstatus == TaskStatus.COMPLETED) {
								spts.setCompletedexecution(true);
								job.getJm().getTaskexcutortasks().get(spts.getTask().getHostport()).add(result);
							}
							spts.getTask().setPiguuid(result.getPiguuid());
							return true;
						} else if (spts.isCompletedexecution() && shouldcontinueprocessing.get()) {
							printresult.acquire();
							if (Objects.isNull(tetotaltaskscompleted.get(spts.getHostPort()))) {
								tetotaltaskscompleted.put(spts.getHostPort(), 0d);
							}
							tetotaltaskscompleted.put(spts.getHostPort(),
									tetotaltaskscompleted.get(spts.getHostPort()) + 1);
							double percentagecompleted = Math.floor((tetotaltaskscompleted.get(spts.getHostPort())
									/ servertotaltasks.get(spts.getHostPort())) * 100.0);
							Utils.writeToOstream(pipelineconfig.getOutput(), "\nPercentage Completed TE("
									+ spts.getHostPort() + ") " + percentagecompleted + "% \n");
							job.getJm().getContainersallocated().put(spts.getHostPort(), percentagecompleted);
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
	 * Calculate the container count via number of processors and container
	 * memory
	 * 
	 * @param blocksize
	 * @param stagecount
	 */
	private void decideContainerCountAndPhysicalMemoryByBlockSize(int stagecount, int blocksize) {
		System.setProperty("jobcount", "1");
		System.setProperty("containercount", "" + pipelineconfig.getNumberofcontainers());
		long containermemory;
		System.setProperty("containercpu", "" + pipelineconfig.getImplicitcontainercpu());
		if (pipelineconfig.getImplicitcontainermemory().equals(DataSamudayaConstants.GB)) {
			containermemory = Long.valueOf(pipelineconfig.getImplicitcontainermemorysize()).longValue() * 1024;
		} else if (pipelineconfig.getImplicitcontainermemory().equals(DataSamudayaConstants.MB)) {
			containermemory = Long.valueOf(pipelineconfig.getImplicitcontainermemorysize()).longValue();
		} else {
			containermemory = Long.valueOf(pipelineconfig.getImplicitcontainermemorysize()).longValue()
					/ DataSamudayaConstants.MB;
		}
		System.setProperty("containermemory", "" + containermemory);
	}

	private final Map<String, StreamPipelineTaskSubmitter> tasksptsthread = new ConcurrentHashMap<>();

	/**
	 * Form a graph for physical execution plan for obtaining the topological
	 * ordering of physical execution plan.
	 * 
	 * @param currentstage
	 * @param nextstage
	 * @param stageoutputs
	 * @param jobid
	 * @param graph
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	public void generatePhysicalExecutionPlan(Stage currentstage, Stage nextstage,
			ConcurrentMap<Stage, Object> stageoutputs, String jobid,
			SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph,
			SimpleDirectedGraph<Task, DAGEdge> taskgraph) throws Exception {
		try {
			if (currentstage.tasks.isEmpty()) {
				return;
			}
			var parentstages = new ArrayList<>(currentstage.parent);
			var parent1stage = !parentstages.isEmpty() ? parentstages.get(0) : null;
			List outputparent1 = null;
			if (parent1stage != null) {
				outputparent1 = (List) stageoutputs.get(parent1stage);
			} else {
				outputparent1 = (List) stageoutputs.get(currentstage);
			}
			var parent2stage = parentstages.size() > 1 ? parentstages.get(1) : null;
			List outputparent2 = null;
			if (parent2stage != null) {
				outputparent2 = (List) stageoutputs.get(parent2stage);
			}
			var tasks = new ArrayList<StreamPipelineTaskSubmitter>();
			var function = currentstage.tasks.get(0);
			// Form the graph for intersection function.
			if (function instanceof IntersectionFunction) {
				if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {					
					partitionindex++;
					List allparents = new ArrayList<>();
					allparents.addAll(outputparent1);
					allparents.addAll(outputparent2);
					var sptsintersection = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
							allparents);
					tasksptsthread.put(sptsintersection.getTask().jobid + sptsintersection.getTask().stageid + sptsintersection.getTask().taskid, sptsintersection);
					sptsintersection.getTask().setTaskspredecessor(new ArrayList<>());
					sptsintersection.getTask().setParentterminatingsize(allparents.size());
					tasks.add(sptsintersection);
					graph.addVertex(sptsintersection);
					taskgraph.addVertex(sptsintersection.getTask());
					for (var parentthread1 : allparents) {
						if (parentthread1 instanceof StreamPipelineTaskSubmitter sptsparent) {
							sptsintersection.getTask().getTaskspredecessor().add(sptsparent.getTask());
							if (!graph.containsVertex(sptsparent)) {
								graph.addVertex(sptsparent);
							}
							if (!taskgraph.containsVertex(sptsparent.getTask())) {
								taskgraph.addVertex(sptsparent.getTask());
							}
							graph.addEdge(sptsparent, sptsintersection);
							taskgraph.addEdge(sptsparent.getTask(), sptsintersection.getTask());
						}
					}
				} else {
					for (var parentthread1 : outputparent2) {
						for (var parentthread2 : outputparent1) {
							partitionindex++;
							StreamPipelineTaskSubmitter spts;
							if ((parentthread1 instanceof BlocksLocation) && (parentthread2 instanceof BlocksLocation)) {
								spts = getPipelineTasks(jobid, Arrays.asList(parentthread1, parentthread2), currentstage,
										partitionindex, currentstage.number, null);
							} else {
								spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
										Arrays.asList(parentthread2, parentthread1));
							}
							tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
							tasks.add(spts);
							graph.addVertex(spts);
							taskgraph.addVertex(spts.getTask());
							if (parentthread1 instanceof StreamPipelineTaskSubmitter sptsparent) {
								if (!graph.containsVertex(sptsparent)) {
									graph.addVertex(sptsparent);
								}
								if (!taskgraph.containsVertex(sptsparent.getTask())) {
									taskgraph.addVertex(sptsparent.getTask());
								}
								graph.addEdge(sptsparent, spts);
								taskgraph.addEdge(sptsparent.getTask(), spts.getTask());
							}
							if (parentthread2 instanceof StreamPipelineTaskSubmitter sptsparent) {
								if (!graph.containsVertex(sptsparent)) {
									graph.addVertex(sptsparent);
								}
								if (!taskgraph.containsVertex(sptsparent.getTask())) {
									taskgraph.addVertex(sptsparent.getTask());
								}
								graph.addEdge(sptsparent, spts);
								taskgraph.addEdge(sptsparent.getTask(), spts.getTask());
							}
						}
					}
				}
			} else if (function instanceof DistributedDistinct) {
				if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {					
					partitionindex++;
					var sptsleft = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
							outputparent1);
					sptsleft.getTask().setIsintersection(true);
					sptsleft.getTask().setTaskspredecessor(new ArrayList<>());
					sptsleft.getTask().parentterminatingsize = outputparent1.size();
					tasksptsthread.put(sptsleft.getTask().jobid + sptsleft.getTask().stageid + sptsleft.getTask().taskid, sptsleft);
					tasks.add(sptsleft);
					graph.addVertex(sptsleft);
					taskgraph.addVertex(sptsleft.getTask());
					for(var parent:outputparent1) {						
						if (parent instanceof StreamPipelineTaskSubmitter sptsparent) {
							sptsleft.getTask().getTaskspredecessor().add(sptsparent.getTask());
							graph.addEdge(sptsparent, sptsleft);
							taskgraph.addEdge(sptsparent.getTask(), sptsleft.getTask());
						}
					}
				}
			} else if (function instanceof UnionFunction) {
				if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
					List allparents = new ArrayList<>();
					allparents.addAll(outputparent1);
					allparents.addAll(outputparent2);
					partitionindex++;
					var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
							allparents);
					spts.getTask().setIsunion(true);
					spts.getTask().setTaskspredecessor(new ArrayList<>());
					tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
					tasks.add(spts);
					graph.addVertex(spts);
					taskgraph.addVertex(spts.getTask());
					for(var parent:allparents) {						
						if (parent instanceof StreamPipelineTaskSubmitter sptsparent) {
							spts.getTask().getTaskspredecessor().add(sptsparent.getTask());
							graph.addEdge(sptsparent, spts);
							taskgraph.addEdge(sptsparent.getTask(), spts.getTask());
						}
					}
				} else {
					if (outputparent1.size() != outputparent2.size()) {
						throw new Exception("Partition Not Equal");
					}
					for (var inputparent1 : outputparent1) {
						for (var inputparent2 : outputparent2) {
							partitionindex++;
							var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
									Arrays.asList(inputparent1, inputparent2));
							tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
							tasks.add(spts);
							graph.addVertex(spts);
							taskgraph.addVertex(spts.getTask());
							if (inputparent1 instanceof StreamPipelineTaskSubmitter input1) {
								if (!graph.containsVertex(input1)) {
									graph.addVertex(input1);
								}
								if (!taskgraph.containsVertex(input1.getTask())) {
									taskgraph.addVertex(input1.getTask());
								}
								graph.addEdge(input1, spts);
								taskgraph.addEdge(input1.getTask(), spts.getTask());
							}
							if (inputparent2 instanceof StreamPipelineTaskSubmitter input2) {
								if (!graph.containsVertex(input2)) {
									graph.addVertex(input2);
								}
								if (!taskgraph.containsVertex(input2.getTask())) {
									taskgraph.addVertex(input2.getTask());
								}
								graph.addEdge(input2, spts);
								taskgraph.addEdge(input2.getTask(), spts.getTask());
							}
						}
					}
				}
			} else if (function instanceof JoinPredicate || function instanceof LeftOuterJoinPredicate
					|| function instanceof RightOuterJoinPredicate || function instanceof Join
					|| function instanceof LeftJoin || function instanceof RightJoin) {
				for (var inputparent1 : outputparent1) {
					for (var inputparent2 : outputparent2) {
						partitionindex++;
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								Arrays.asList(inputparent1, inputparent2));
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						tasks.add(spts);
						graph.addVertex(spts);
						taskgraph.addVertex(spts.getTask());
						if (inputparent1 instanceof StreamPipelineTaskSubmitter input1) {
							input1.getTask().setJoinpos("left");
							if (!graph.containsVertex(input1)) {
								graph.addVertex(input1);
							}
							if (!taskgraph.containsVertex(input1.getTask())) {
								taskgraph.addVertex(input1.getTask());
							}
							graph.addEdge(input1, spts);
							taskgraph.addEdge(input1.getTask(), spts.getTask());
						} else if (inputparent1 instanceof Task task) {
							spts.getTask().setJoinpos("left");
							taskgraph.addVertex(task);
							taskgraph.addVertex(spts.getTask());
							taskgraph.addEdge(task, spts.getTask());
						}
						if (inputparent2 instanceof StreamPipelineTaskSubmitter input2) {
							input2.getTask().setJoinpos("right");
							if (!graph.containsVertex(input2)) {
								graph.addVertex(input2);
							}
							if (!taskgraph.containsVertex(input2.getTask())) {
								taskgraph.addVertex(input2.getTask());
							}
							graph.addEdge(input2, spts);
							taskgraph.addEdge(input2.getTask(), spts.getTask());
						} else if (inputparent2 instanceof Task task) {
							task.setJoinpos("right");
							taskgraph.addVertex(task);
							taskgraph.addVertex(spts.getTask());
							taskgraph.addEdge(task, spts.getTask());
						}
					}
				}
			} else if (function instanceof Coalesce coalesce) {
				if (islocal || isignite || isyarn || ismesos) {
					var partkeys = Iterables
							.partition(outputparent1, (outputparent1.size()) / coalesce.getCoalescepartition())
							.iterator();
					for (; partkeys.hasNext();) {
						var parentpartitioned = (List) partkeys.next();
						partitionindex++;
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								parentpartitioned);
						tasks.add(spts);
						graph.addVertex(spts);
						taskgraph.addVertex(spts.getTask());
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						for (var input : parentpartitioned) {
							var parentthread = (StreamPipelineTaskSubmitter) input;
							if (!graph.containsVertex(parentthread)) {
								graph.addVertex(parentthread);
							}
							if (!taskgraph.containsVertex(parentthread.getTask())) {
								taskgraph.addVertex(parentthread.getTask());
							}
							graph.addEdge(parentthread, spts);
							taskgraph.addEdge(parentthread.getTask(), spts.getTask());
						}
					}
				} else {
					var parentpartitioned = (List<StreamPipelineTaskSubmitter>) outputparent1;

					Map<String, List<StreamPipelineTaskSubmitter>> hpspts = parentpartitioned.stream()
							.collect(Collectors.groupingBy(sptsmap -> sptsmap.getHostPort(), HashMap::new,
									Collectors.mapping(sptsmap -> sptsmap, Collectors.toList())));

					for (Entry<String, List<StreamPipelineTaskSubmitter>> entry : hpspts.entrySet()) {
						var partkeys = Iterables.partition(entry.getValue(),
								(entry.getValue().size()) / coalesce.getCoalescepartition()).iterator();
						for (; partkeys.hasNext();) {
							partitionindex++;
							var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
									(List) entry.getValue());
							tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
									spts);
							tasks.add(spts);
							graph.addVertex(spts);
							taskgraph.addVertex(spts.getTask());
							for (var input : partkeys.next()) {
								var parentthread = (StreamPipelineTaskSubmitter) input;
								if (!graph.containsVertex(parentthread)) {
									graph.addVertex(parentthread);
								}
								if (!taskgraph.containsVertex(parentthread.getTask())) {
									taskgraph.addVertex(parentthread.getTask());
								}
								graph.addEdge(parentthread, spts);
								taskgraph.addEdge(parentthread.getTask(), spts.getTask());
							}
						}
					}
				}
			} else if (function instanceof AggregateReduceFunction) {
				partitionindex++;
				var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
						new ArrayList<>(outputparent1));
				tasks.add(spts);
				graph.addVertex(spts);
				taskgraph.addVertex(spts.getTask());
				tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
				for (var input : outputparent1) {
					var parentthread = (StreamPipelineTaskSubmitter) input;
					if (!graph.containsVertex(parentthread)) {
						graph.addVertex(parentthread);
					}
					if (!taskgraph.containsVertex(parentthread.getTask())) {
						taskgraph.addVertex(parentthread.getTask());
					}
					graph.addEdge(parentthread, spts);
					taskgraph.addEdge(parentthread.getTask(), spts.getTask());
				}
			} else if (function instanceof ShuffleStage) {
				partitionindex++;
				Map<Integer, FilePartitionId> filepartitionsid = new ConcurrentHashMap<>();
				Map<String, StreamPipelineTaskSubmitter> taskexecshuffleblockmap = new ConcurrentHashMap<>();
				int nooffilepartitions = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC, 
						DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
				int startrange = 0;
				int endrange = nooffilepartitions;
				List<StreamPipelineTaskSubmitter> parents = outputparent1;
				if (CollectionUtils.isNotEmpty(job.getTaskexecutors())) {
					Map<String, List<Object>> hpsptsl = parents.stream()
							.collect(Collectors.groupingBy(StreamPipelineTaskSubmitter::getHostPort,
									Collectors.mapping(spts -> spts, Collectors.toList())));
					for (Entry<String, List<Object>> entry : hpsptsl.entrySet()) {
						int initialrange = startrange;
						for (; initialrange < endrange; initialrange++) {
							filepartitionsid.put(initialrange, new FilePartitionId(Utils.getUUID(),
									DataSamudayaConstants.EMPTY, startrange, endrange, initialrange));
						}
						startrange += nooffilepartitions;
						endrange += nooffilepartitions;
						var parentsgraph = hpsptsl.get(entry.getKey());
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								parentsgraph);
						spts.getTask().parentterminatingsize = outputparent1.size();
						tasks.add(spts);
						graph.addVertex(spts);
						for (var parent : parentsgraph) {
							StreamPipelineTaskSubmitter parentthread = (StreamPipelineTaskSubmitter) parent;
							parentthread.getTask().filepartitionsid = filepartitionsid;
							spts.getTask().filepartitionsid = filepartitionsid;
							tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
									spts);
							taskgraph.addVertex(spts.getTask());
							if (!graph.containsVertex(parentthread)) {
								graph.addVertex(parentthread);
							}
							if (!taskgraph.containsVertex(parentthread.getTask())) {
								taskgraph.addVertex(parentthread.getTask());
							}
							graph.addEdge(parentthread, spts);
							taskgraph.addEdge(parentthread.getTask(), spts.getTask());
						}
					}
					List<Task> tasksshuffle = tasks.stream().map(spts -> spts.getTask()).collect(Collectors.toList());
					for (var input : outputparent1) {
						if (input instanceof StreamPipelineTaskSubmitter parentthread) {
							parentthread.getTask().setShufflechildactors(tasksshuffle);
						}
					}
				} else {
					for (int filepartcount = 0; filepartcount < 1; filepartcount++) {
						int initialrange = startrange;
						for (; initialrange < endrange; initialrange++) {
							filepartitionsid.put(initialrange, new FilePartitionId(Utils.getUUID(),
									DataSamudayaConstants.EMPTY, startrange, endrange, initialrange));
						}
						startrange += nooffilepartitions;
						endrange += nooffilepartitions;
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								outputparent1);
						spts.getTask().parentterminatingsize = parents.size();
						tasks.add(spts);
						graph.addVertex(spts);
						for (var parent : outputparent1) {
							StreamPipelineTaskSubmitter parentthread = (StreamPipelineTaskSubmitter) parent;
							parentthread.getTask().filepartitionsid = filepartitionsid;
							spts.getTask().filepartitionsid = filepartitionsid;
							spts.getTask().parentterminatingsize = outputparent1.size();
							tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
									spts);
							taskgraph.addVertex(spts.getTask());
							if (!graph.containsVertex(parentthread)) {
								graph.addVertex(parentthread);
							}
							if (!taskgraph.containsVertex(parentthread.getTask())) {
								taskgraph.addVertex(parentthread.getTask());
							}
							graph.addEdge(parentthread, spts);
							taskgraph.addEdge(parentthread.getTask(), spts.getTask());
						}
					}
					List<Task> tasksshuffle = tasks.stream().map(spts -> spts.getTask()).collect(Collectors.toList());
					for (var input : outputparent1) {
						if (input instanceof StreamPipelineTaskSubmitter parentthread) {
							parentthread.getTask().setShufflechildactors(tasksshuffle);
						}
					}
				}
			} else if (function instanceof HashPartitioner || function instanceof GroupByFunction
					|| function instanceof ReduceByKeyFunction || function instanceof ReduceByKeyFunctionValues
					|| function instanceof ReduceFunction) {
				partitionindex++;
				for (var input : outputparent1) {
					if (input instanceof Task task) {
						var spts = getPipelineTasks(jobid, input, currentstage, partitionindex, currentstage.number,
								null);
						tasks.add(spts);
						graph.addVertex(spts);
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						taskgraph.addVertex(task);
						taskgraph.addVertex(spts.getTask());
						taskgraph.addEdge(task, spts.getTask());
					} else if (input instanceof StreamPipelineTaskSubmitter parentthread) {
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								Arrays.asList(input));
						tasks.add(spts);
						graph.addVertex(spts);
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						taskgraph.addVertex(spts.getTask());
						if (!graph.containsVertex(parentthread)) {
							graph.addVertex(parentthread);
						}
						if (!taskgraph.containsVertex(parentthread.getTask())) {
							taskgraph.addVertex(parentthread.getTask());
						}
						graph.addEdge(parentthread, spts);
						taskgraph.addEdge(parentthread.getTask(), spts.getTask());
					}

				}
			} else if (function instanceof DistributedSort){
				var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
						outputparent1);
				tasks.add(spts);
				graph.addVertex(spts);
				spts.getTask().setTaskspredecessor(new ArrayList<>());
				spts.getTask().setTosort(true);
				tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
				StreamPipelineTaskSubmitter parentspts = (StreamPipelineTaskSubmitter) outputparent1.get(0);
				JobStage js = jsidjsmap.get(parentspts.getTask().getJobid() + parentspts.getTask().getStageid());
				Object sortedcomparator = js.getStage().getTasks().get(js.getStage().getTasks().size()-1);
				if(sortedcomparator instanceof FieldCollatedSortedComparator fcsc) {
					spts.getTask().setFcsc(fcsc.getRfcs().stream().map(fc->new FieldCollationDirection(fc.getFieldIndex(), fc.getDirection())).toList());
				} else if(sortedcomparator instanceof PigSortedComparator psc) {
					spts.getTask().setFcsc(psc.getCso().stream().map(cso->new FieldCollationDirection(cso.getColumn(), cso.isIsasc()?Direction.ASCENDING:Direction.DESCENDING)).toList());
				}
				spts.getTask().setTeid(pipelineconfig.getTejobid());
				for(var parentthread: (List<StreamPipelineTaskSubmitter>)outputparent1) {
					spts.getTask().getTaskspredecessor().add(parentthread.getTask());
					taskgraph.addVertex(spts.getTask());
					if (!graph.containsVertex(parentthread)) {
						graph.addVertex(parentthread);
					}
					if (!taskgraph.containsVertex(parentthread.getTask())) {
						taskgraph.addVertex(parentthread.getTask());
					}
					graph.addEdge(parentthread, spts);
					taskgraph.addEdge(parentthread.getTask(), spts.getTask());
				}
			} else {
				// Form the nodes and edges for map stage.
				for (var input : outputparent1) {
					partitionindex++;
					StreamPipelineTaskSubmitter spts;
					if (input instanceof BlocksLocation) {
						spts = getPipelineTasks(jobid, input, currentstage, partitionindex, currentstage.number, null);
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						graph.addVertex(spts);
						taskgraph.addVertex(spts.getTask());
					} else if (input instanceof Task task) {
						spts = getPipelineTasks(jobid, task, currentstage, partitionindex, currentstage.number, null);
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						graph.addVertex(spts);
						taskgraph.addVertex(task);
						taskgraph.addVertex(spts.getTask());
						taskgraph.addEdge(task, spts.getTask());
					} else {
						var parentthread = (StreamPipelineTaskSubmitter) input;
						spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								Arrays.asList(parentthread));
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						graph.addVertex(spts);
						taskgraph.addVertex(spts.getTask());
						if (!graph.containsVertex(parentthread)) {
							graph.addVertex(parentthread);
						}
						if (!taskgraph.containsVertex(parentthread.getTask())) {
							taskgraph.addVertex(parentthread.getTask());
						}
						graph.addEdge(parentthread, spts);
						taskgraph.addEdge(parentthread.getTask(), spts.getTask());
					}
					tasks.add(spts);
				}
			}
			stageoutputs.put(currentstage, tasks);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERPHYSICALEXECUTIONPLANERROR, ex);
		}
	}

	/**
	 * Get final stages which has no successors.
	 * 
	 * @param graph
	 * @param sptsl
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set<StreamPipelineTaskSubmitter> getFinalPhasesWithNoSuccessors(Graph graph,
			List<StreamPipelineTaskSubmitter> sptsl) {
		return sptsl.stream().filter(spts -> Graphs.successorListOf(graph, spts).isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Get initial stages which has no predecessors.
	 * 
	 * @param graph
	 * @param sptsl
	 * @return
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public Set<StreamPipelineTaskSubmitter> getFinalPhasesWithNoPredecessors(Graph graph,
			List<StreamPipelineTaskSubmitter> sptsl) {
		return sptsl.stream().filter(spts -> Graphs.predecessorListOf(graph, spts).isEmpty())
				.collect(Collectors.toCollection(LinkedHashSet::new));
	}

	/**
	 * Obtain the final stage output as when statuses for final stage been
	 * completed.
	 * 
	 * @param graph
	 * @param sptsl
	 * @param ismesos
	 * @param isyarn
	 * @return
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "rawtypes" })
	public List getLastStageOutput(Set<StreamPipelineTaskSubmitter> sptss,
			Graph<StreamPipelineTaskSubmitter, DAGEdge> graph, List<StreamPipelineTaskSubmitter> sptsl, Boolean ismesos,
			Boolean isyarn, Boolean islocal, Boolean isjgroups, ConcurrentMap<String, OutputStream> resultstream,
			Graph<Task, DAGEdge> taskgraph) throws PipelineException {
		log.debug("HDFS Path TO Retrieve Final Task Output: " + hdfsfilepath);

		try {
			log.debug("Final Stages: " + sptss);
			Utils.writeToOstream(pipelineconfig.getOutput(), "Final Stages: " + sptss);

			if (Boolean.TRUE.equals(isignite)) {
				int partition = 0;
				for (var spts : sptss) {
					job.getOutput().add(spts);
					// Get final stage results from ignite
					writeResultsFromIgnite(spts.getTask(), partition++, stageoutput);
				}
			} else if (Boolean.TRUE.equals(islocal)) {
				if (job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE) {
					if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
						for (var spts : sptss) {
							var key = getIntermediateResultFS(spts.getTask());
							while (isNull(jobidstageidtaskidcompletedmap.get(key))
									|| !jobidstageidtaskidcompletedmap.get(key)) {
								Thread.sleep(1000);
							}
							var databytes = (byte[]) cache.get(key);
							try (var bais = new ByteArrayInputStream(databytes);
									var sis = new SnappyInputStream(bais);
									var input = new Input(sis);) {
								var obj = Utils.getKryo().readClassAndObject(input);
								if(obj instanceof List lst && CollectionUtils.isNotEmpty(lst) && lst.get(0) instanceof NodeIndexKey) {
									List larrayobj = new ArrayList<>();
									Stream stream = Utils.getStreamData(lst, cache);
									stream.map(new MapFunction<Object[], Object[]>(){
										private static final long serialVersionUID = 4827381029527934511L;

										public Object[] apply(Object[] values) {
											return values[0].getClass() == Object[].class ? (Object[]) values[0] : values;
										}
									}).forEach(larrayobj::add);
									stageoutput.add(larrayobj);
								} else {
									resultstream.remove(key);
									stageoutput.add(obj);
								}
							} catch (Exception ex) {
								log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
								throw ex;
							}
						}
					} else {
						for (var spts : sptss) {
							var key = getIntermediateResultFS(spts.getTask());
							try (var fsstream = resultstream.get(key);
									ByteBufferInputStream bbis = new ByteBufferInputStream(
											((ByteBufferOutputStream) fsstream).get());
									var input = new Input(bbis);) {
								var obj = Utils.getKryo().readClassAndObject(input);
								;
								resultstream.remove(key);
								writeOutputToFile(stageoutput.size(), obj);
								stageoutput.add(obj);
							} catch (Exception ex) {
								log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
								throw ex;
							}
						}
					}
				}
			} else if (Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn)) {
				int partition = 0;
				if (Boolean.TRUE.equals(isyarn) && job.getTrigger() == TRIGGER.PIGDUMP) {
					var vertices = taskgraph.vertexSet();
					var tasks = vertices.stream().filter(spts -> Graphs.successorListOf(taskgraph, spts).isEmpty())
							.collect(Collectors.toCollection(LinkedHashSet::new));
					PrintWriter out = new PrintWriter(pipelineconfig.getPigoutput(), true);
					long totalrecords = 0;
					for (var task : tasks) {
						// Get final stage results mesos or yarn
						writeOutputToHDFS(hdfs, task, partition++, stageoutput);
						totalrecords += Utils.printTableOrError((List) stageoutput.get(stageoutput.size() - 1), out,
								JOBTYPE.PIG);
					}
					out.println();
					out.printf("Total records processed %d", totalrecords);
					out.println();
					out.flush();
				} else if (job.getTrigger() != TRIGGER.SAVERESULTSTOFILE) {
					for (var spts : sptss) {
						// Get final stage results mesos or yarn
						writeOutputToHDFS(hdfs, spts.getTask(), partition++, stageoutput);
					}
				}
			} else {
				var ishdfs = false;
				if (nonNull(job.getUri())) {
					ishdfs = new URL(job.getUri()).getProtocol().equals(DataSamudayaConstants.HDFS_PROTOCOL);
				}
				sptss = getFinalPhasesWithNoSuccessors(graph,
						new ArrayList<StreamPipelineTaskSubmitter>(graph.vertexSet()));
				long totalrecords = 0;
				for (var spts : sptss) {
					// Get final stage results
					if (spts.isCompletedexecution() && job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE
							|| !ishdfs) {
						Task task = spts.getTask();
						RemoteDataFetch rdf = new RemoteDataFetch();
						rdf.setHp(task.hostport);
						rdf.setJobid(task.jobid);
						rdf.setStageid(task.stageid);
						rdf.setTaskid(task.taskid);
						rdf.setTejobid(task.jobid);
						rdf.setStorage(pipelineconfig.getStorage());
						if (pipelineconfig.getUseglobaltaskexecutors()) {
							rdf.setTejobid(pipelineconfig.getTejobid());
						}
						boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
						rdf.setMode(isJGroups ? DataSamudayaConstants.JGROUPS : DataSamudayaConstants.STANDALONE);
						RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
						try (var input = new Input(pipelineconfig.getStorage() == STORAGE.INMEMORY || isjgroups
								? new ByteArrayInputStream(rdf.getData())
								: new SnappyInputStream(new ByteArrayInputStream(rdf.getData())));) {
							var result = Utils.getKryo().readClassAndObject(input);
							if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL && (job.getJobtype() == JOBTYPE.NORMAL 
									|| job.getJobtype() == JOBTYPE.PIG)) {
								PrintWriter out = pipelineconfig.getWriter();
								if(result instanceof List lst && CollectionUtils.isNotEmpty(lst) && lst.get(0) instanceof NodeIndexKey) {
									if(task.isIsunion() || task.isIsintersection()) {
										int diskexceedpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
												DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
										DiskSpillingSet<NodeIndexKey> diskspillset = new DiskSpillingSet(task, diskexceedpercentage, null, false,false ,false, null, null, 1);
										for (NodeIndexKey nik : (List<NodeIndexKey>)lst) {
											log.info("Getting Next List From Remote Server with FCD {}", nik.getTask().getFcsc());
											try (RemoteIteratorClient client = new RemoteIteratorClient(nik.getTask(), nik.getTask().getFcsc(),
													RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
												while (client.hasNext()) {
													log.info("Getting Next List From Remote Server");
													List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
															.convertBytesToObjectCompressed((byte[]) client.next(), null);
													log.info("Next List From Remote Server with size {}", niks.size());
													niks.stream().parallel().forEach(diskspillset::add);
												}
											}
										}
										List larrayobj = new ArrayList<>();										
										if(diskspillset.isSpilled()) {
											diskspillset.close();
										}
										Stream<NodeIndexKey> datastream = diskspillset.isSpilled()
												? (Stream<NodeIndexKey>) Utils.getStreamData(new FileInputStream(
												Utils.getLocalFilePathForTask(diskspillset.getTask(), null, false, false, false)))
												: diskspillset.getData().stream();
										datastream.map(nik->nik.getKey()).map(new MapFunction<Object[], Object[]>(){
											private static final long serialVersionUID = -6478016520828716284L;
	
											public Object[] apply(Object[] values) {
												return values[0].getClass() == Object[].class ? (Object[]) values[0] : values;
											}
										}).forEach(larrayobj::add);
										diskspillset.clear();
										if(nonNull(out)) {
											totalrecords += Utils.printTableOrError((List) larrayobj, out, JOBTYPE.PIG);
										} else {
											stageoutput.add(larrayobj);
										}
									} else if(task.isTosort()) {
										int btreesize = Integer.valueOf(DataSamudayaProperties.get().getProperty(
												DataSamudayaConstants.BTREEELEMENTSNUMBER, DataSamudayaConstants.BTREEELEMENTSNUMBER_DEFAULT));
										BTree btree = new BTree(btreesize);
										for (NodeIndexKey nik : (List<NodeIndexKey>)lst) {
											log.info("Getting Next List From Remote Server with FCD {}", nik.getTask().getFcsc());
											try (RemoteIteratorClient client = new RemoteIteratorClient(nik.getTask(), nik.getTask().getFcsc(),
													RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
												while (client.hasNext()) {
													log.info("Getting Next List From Remote Server");
													List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
															.convertBytesToObjectCompressed((byte[]) client.next(), null);
													log.info("Next List From Remote Server with size {}", niks.size());
													for (NodeIndexKey niktree : niks) {
														niktree.setTask(nik.getTask());
														btree.insert(niktree, nik.getTask().getFcsc());
													}
												}
											}
										}
										List larrayobj = new ArrayList<>();
										List<NodeIndexKey> rootniks = new ArrayList<>();
										btree.traverse(rootniks);
										Stream stream = Utils.getStreamData(rootniks, null);
										stream.map(new MapFunction<Object[], Object[]>(){
											private static final long serialVersionUID = -6478016520828716284L;
	
											public Object[] apply(Object[] values) {
												return values[0].getClass() == Object[].class ? (Object[]) values[0] : values;
											}
										}).forEach(larrayobj::add);									
										if(nonNull(out)) {
											totalrecords += Utils.printTableOrError((List) larrayobj, out, JOBTYPE.PIG);
										} else {
											stageoutput.add(larrayobj);
										}
									}
								} else {
									if(nonNull(out)) {
										totalrecords += Utils.printTableOrError((List) result, out, JOBTYPE.PIG);
									} else {
										stageoutput.add(result);
									}
								}								
							} else {
								writeOutputToFile(stageoutput.size(), result);
								stageoutput.add(result);
							}
						} catch (Exception ex) {
							log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
							throw ex;
						}
					}
				}
				if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL && (job.getJobtype() == JOBTYPE.NORMAL 
						|| job.getJobtype() == JOBTYPE.PIG)
						&& nonNull(pipelineconfig.getWriter())) {
					PrintWriter out = pipelineconfig.getWriter();
					out.println();
					out.printf("Total records processed %d", totalrecords);
					out.println();
					out.flush();
				}
			}
			sptss.clear();
			sptss = null;
			return stageoutput;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
		}

	}

	/**
	 * Writes output to File
	 * 
	 * @param partcount
	 * @param result
	 * @throws PipelineException
	 * @throws MalformedURLException
	 */
	@SuppressWarnings("rawtypes")
	public void writeOutputToFile(int partcount, Object result) throws PipelineException, MalformedURLException {
		if (job.getTrigger() == job.getTrigger().SAVERESULTSTOFILE) {
			URL url = new URL(job.getUri());
			boolean isfolder = url.getProtocol().equals(DataSamudayaConstants.FILE);
			try (OutputStream fsdos = isfolder
					? new FileOutputStream(url.getPath() + DataSamudayaConstants.FORWARD_SLASH + job.getSavepath()
							+ DataSamudayaConstants.HYPHEN + partcount)
					: hdfs.create(new Path(
							job.getUri().toString() + job.getSavepath() + DataSamudayaConstants.HYPHEN + partcount));
					BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos))) {

				if (result instanceof List res) {
					Utils.convertToCsv(res, fsdos);
				} else {
					Utils.convertToCsv(Arrays.asList(result), fsdos);
				}
				bw.flush();
				fsdos.flush();
			} catch (Exception ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			}
		}
	}

	/**
	 * Writes output to file from inmemory intermediate output.
	 * 
	 * @param spts
	 * @param stageoutput
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void writeOutputToFileInMemory(StreamPipelineTaskSubmitter spts, List stageoutput) throws PipelineException {
		try (var fsstream = getIntermediateInputStreamInMemory(spts.getTask()); var input = new Input(fsstream);) {
			var obj = Utils.getKryo().readClassAndObject(input);
			;
			writeOutputToFile(stageoutput.size(), obj);
			stageoutput.add(obj);
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
		}
	}

	/**
	 * Get the input streams from task object.
	 * 
	 * @param task
	 * @return input stream
	 * @throws Exception
	 */
	private InputStream getIntermediateInputStreamInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.setJobid(task.jobid);
			rdf.setStageid(task.stageid);
			rdf.setTaskid(task.taskid);
			rdf.setHp(task.hostport);
			rdf.setTejobid(task.jobid);
			if (pipelineconfig.getUseglobaltaskexecutors()) {
				rdf.setTejobid(pipelineconfig.getTejobid());
			}
			boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
			rdf.setMode(isJGroups ? DataSamudayaConstants.JGROUPS : DataSamudayaConstants.STANDALONE);
			RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
			return new SnappyInputStream(new ByteArrayInputStream(rdf.getData()));
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	public String getIntermediateDataFSFilePath(String jobid, String stageid, String taskid) {
		return jobid + DataSamudayaConstants.HYPHEN + stageid + DataSamudayaConstants.HYPHEN + taskid;
	}

	/**
	 * Get the file streams from HDFS and jobid and stageid.
	 * 
	 * @param hdfs
	 * @return
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	private void writeOutputToHDFS(FileSystem hdfs, Task task, int partition, List stageoutput) throws Exception {
		try {
			var path = DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH
					+ task.jobid + DataSamudayaConstants.FORWARD_SLASH + task.taskid;
			log.debug("Forming URL Final Stage:" + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
					+ DataSamudayaConstants.FORWARD_SLASH + task.jobid + DataSamudayaConstants.FORWARD_SLASH
					+ task.taskid);
			try (var input = hdfs.open(new Path(path));) {
				byte[] result = input.readAllBytes();
				try (var objectinput = new Input(new ByteArrayInputStream(result))) {
					stageoutput.add(Utils.getKryo().readClassAndObject(objectinput));
				}
			} catch (Exception ex) {
				log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
			}

		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERHDFSDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get the rdf object from task object.
	 * 
	 * @return rdf object
	 * @throws Exception
	 */
	private RemoteDataFetch getIntermediateRdfInMemory(Task task) throws Exception {
		try {
			var rdf = new RemoteDataFetch();
			rdf.setJobid(task.jobid);
			rdf.setStageid(task.stageid);
			rdf.setTaskid(task.taskid);
			rdf.setHp(task.hostport);
			rdf.setTejobid(task.jobid);
			if (pipelineconfig.getUseglobaltaskexecutors()) {
				rdf.setTejobid(pipelineconfig.getTejobid());
			}
			boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
			rdf.setMode(isJGroups ? DataSamudayaConstants.JGROUPS : DataSamudayaConstants.STANDALONE);
			return rdf;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	/**
	 * Get intermediate stream obtained from ignite server.
	 * 
	 * @param task
	 * @param partition
	 * @param stageoutput
	 * @throws Exception
	 */
	@SuppressWarnings({ "rawtypes", "unchecked" })
	private void writeResultsFromIgnite(Task task, int partition, List stageoutput) throws Exception {
		try {
			log.info("Eventual outcome Ignite task: " + task);

			try (var sis = new ByteArrayInputStream(job.getIgcache().get(task.jobid + task.stageid + task.taskid));
					var input = new Input(sis);) {
				var obj = Utils.getKryo().readClassAndObject(input);
				;
				if (!Objects.isNull(job.getUri())) {
					job.setTrigger(job.getTrigger().SAVERESULTSTOFILE);
					writeOutputToFile(partition, obj);
				}
				if (job.getJobtype() == JOBTYPE.PIG) {
					PrintWriter out = new PrintWriter(pipelineconfig.getPigoutput());
					long totalrecords = 0;
					totalrecords += Utils.printTableOrError((List) obj, out, JOBTYPE.PIG);
					out.println();
					out.printf("Total records processed %d", totalrecords);
					out.println();
					out.flush();
				} else {
					stageoutput.add(obj);
				}
			} catch (Exception ex) {
				log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERINMEMORYDATAFETCHERROR, ex);
		}
	}

	private String getIntermediateResultFS(Task task) throws Exception {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}

	private int partitionindex;

	/**
	 * Get the stream thread in order to execute the tasks.
	 * 
	 * @param jobid
	 * @param input
	 * @param stage
	 * @param partitionindex
	 * @param currentstage
	 * @param parentthreads
	 * @return spts object
	 * @throws PipelineException
	 */
	@SuppressWarnings("rawtypes")
	private StreamPipelineTaskSubmitter getPipelineTasks(String jobid, Object input, Stage stage, int partitionindex,
			int currentstage, List<Object> parentthreads) throws PipelineException {
		try {
			var task = new Task();
			task.setTopersist(pipelineconfig.isTopersistcolumnar());
			task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN
					+ job.getTaskidgenerator().getAndIncrement());
			task.jobid = jobid;
			task.stageid = stage.id;
			task.storage = pipelineconfig.getStorage();
			task.teid = pipelineconfig.getTejobid();
			String hp = null;
			task.hbphysicaladdress = hbphysicaladdress;
			if (currentstage == 0 || parentthreads == null) {
				task.parentremotedatafetch = null;
				if (input instanceof List inputl) {
					task.input = inputl.toArray();
					hp = ((BlocksLocation) task.input[0]).getExecutorhp();
				} else if (input instanceof BlocksLocation bl) {
					hp = bl.getExecutorhp();
					task.input = new Object[] { input };
				} else if (input instanceof Task inputtask) {
					hp = inputtask.getHostport();
					task.input = new Object[] { inputtask };
					task.parentremotedatafetch = new RemoteDataFetch[1];
				}

			} else {
				task.input = new Object[parentthreads.size()];
				task.parentremotedatafetch = new RemoteDataFetch[parentthreads.size()];
				for (var parentcount = 0; parentcount < parentthreads.size(); parentcount++) {
					if (parentthreads.get(parentcount) instanceof StreamPipelineTaskSubmitter spts) {
						if (!isignite) {
							task.parentremotedatafetch[parentcount] = new RemoteDataFetch();
							task.parentremotedatafetch[parentcount].setJobid(spts.getTask().jobid);
							task.parentremotedatafetch[parentcount].setStageid(spts.getTask().stageid);
							task.parentremotedatafetch[parentcount].setTaskid(spts.getTask().taskid);
							task.parentremotedatafetch[parentcount].setHp(spts.getHostPort());
							task.parentremotedatafetch[parentcount].setTejobid(task.jobid);
							if (pipelineconfig.getUseglobaltaskexecutors()) {
								task.parentremotedatafetch[parentcount].setTejobid(pipelineconfig.getTejobid());
							}
							if (Boolean.parseBoolean(pipelineconfig.getJgroups())) {
								task.parentremotedatafetch[parentcount].setMode(DataSamudayaConstants.JGROUPS);
							} else {
								task.parentremotedatafetch[parentcount].setMode(DataSamudayaConstants.STANDALONE);
							}
							hp = spts.getHostPort();
						} else {
							task.input[parentcount] = spts.getTask();
						}
					} else if (parentthreads.get(parentcount) instanceof BlocksLocation bl) {
						task.input[parentcount] = bl;
						hp = bl.getExecutorhp();
					} else if (isignite && parentthreads.get(parentcount) instanceof Task insttask) {
						task.input[parentcount] = insttask;
					}
				}
			}
			var spts = new StreamPipelineTaskSubmitter(task, hp, job.getPipelineconfig());
			task.hostport = hp;
			zo.createTasksForJobNode(jobid, task, event -> {
				var taskid = task.taskid;
				log.info("Task {} created in zookeeper", taskid);
			});
			return spts;
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERCREATINGSTREAMSCHEDULERTHREAD, ex);
		}
	}

	public void ping(Job job) throws Exception {
		chtssha = TssHAChannel.tsshachannel;
		jobping.execute(() -> {
			while (!istaskcancelled.get()) {
				try (var baos = new ByteArrayOutputStream();
						var lzf = new SnappyOutputStream(baos);
						var output = new Output(lzf);) {
					job.setPipelineconfig((PipelineConfig) job.getPipelineconfig().clone());
					job.getPipelineconfig().setOutput(null);
					Utils.getKryo().writeClassAndObject(output, job);
					chtssha.send(new ObjectMessage(null, baos.toByteArray()));
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					log.warn("Interrupted!", e);
					// Restore interrupted state...
					Thread.currentThread().interrupt();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
					break;
				}
			}
		});
	}
}
