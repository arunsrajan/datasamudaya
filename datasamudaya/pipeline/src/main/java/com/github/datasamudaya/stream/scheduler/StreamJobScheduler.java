/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.stream.scheduler;

import static java.util.Objects.nonNull;

import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.io.StringWriter;
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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.ehcache.Cache;
import org.jgrapht.Graph;
import org.jgrapht.Graphs;
import org.jgrapht.graph.SimpleDirectedGraph;
import org.jgrapht.traverse.TopologicalOrderIterator;
import org.jgroups.JChannel;
import org.jgroups.ObjectMessage;
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
import com.github.datasamudaya.common.CloseStagesGraphExecutor;
import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.FreeResourcesCompletedJob;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.Job.JOBTYPE;
import com.github.datasamudaya.common.Job.TRIGGER;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TasksGraphExecutor;
import com.github.datasamudaya.common.TssHAChannel;
import com.github.datasamudaya.common.TssHAHostPorts;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.Join;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftJoin;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.RightJoin;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutor;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorIgnite;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorIgniteSQL;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorLocal;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorLocalSQL;
import com.github.datasamudaya.stream.mesos.scheduler.MesosScheduler;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.TaskProvider;
import com.google.common.collect.Iterables;

/**
 * 
 * @author Arun 
 * Schedule the jobs for parallel execution to task executor for standalone application
 * or mesos scheduler and executor or yarn scheduler i.e app master and executor.
 */
public class StreamJobScheduler {

  private static org.slf4j.Logger log = LoggerFactory.getLogger(StreamJobScheduler.class);

  public int batchsize;
  public Set<String> taskexecutors;
  private Semaphore yarnmutex = new Semaphore(1);
  public static ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();;
  @SuppressWarnings("rawtypes")
  Cache cache;
  public Semaphore semaphore;

  public PipelineConfig pipelineconfig;
  AtomicBoolean istaskcancelled = new AtomicBoolean();
  public Map<String, JobStage> jsidjsmap = new ConcurrentHashMap<>();
  public List<Object> stageoutput = new ArrayList<>();
  String hdfsfilepath;
  FileSystem hdfs;
  String hbphysicaladdress;

  public StreamJobScheduler() {
    hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
        DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
  }

  ExecutorService jobping = Executors.newWorkStealingPool();
  public Job job;
  public Boolean islocal;
  public Boolean isignite,ismesos,isyarn,isjgroups;
  JChannel chtssha;
  SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge> graph;
  public ZookeeperOperations zo = null;
  /**
   * Schedule the job for parallelization
   * 
   * @param job
   * @return
   * @throws Exception
   * @throws Throwable
   */
  @SuppressWarnings({"unchecked", "rawtypes", "resource"})
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
      // for standalone DATASAMUDAYA task schedulers and executors to communicate
      // via task statuses.
      if (Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn)
          && Boolean.FALSE.equals(islocal) && Boolean.FALSE.equals(isjgroups) && !isignite) {
        // Initialize the heart beat for gathering the resources
        // Initialize the heart beat for gathering the task executors
        // task statuses information.
    	if(job.getPipelineconfig().getIsremotescheduler()) {
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
    	  if(job.getPipelineconfig().getIsremotescheduler()) {
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
          generatePhysicalExecutionPlan(stage, nextstage, job.getStageoutputmap(), job.getId(),
              graph, taskgraph);
          stagenumber++;
        }
        job.setVertices(new LinkedHashSet<>(graph.vertexSet()));
        job.setEdges(new LinkedHashSet<>(graph.edgeSet()));
      } else {
        job.getVertices().stream()
            .forEach(vertex -> graph.addVertex((StreamPipelineTaskSubmitter) vertex));
        job.getEdges().stream()
            .forEach(edge -> graph.addEdge((StreamPipelineTaskSubmitter) edge.getSource(),
                (StreamPipelineTaskSubmitter) edge.getTarget()));
      }
      batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
      semaphore = new Semaphore(batchsize);
      var writer = new StringWriter();
      if (Boolean.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
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
      Utils.writeToOstream(pipelineconfig.getOutput(), "stages: " + sptsl);
      if (isignite) {
        parallelExecutionPhaseIgnite(graph, new TaskProviderIgnite());
      }
      // If local scheduler
      else if (Boolean.TRUE.equals(islocal)) {
        job.setResultstream(resultstream);
        batchsize = Integer.parseInt(pipelineconfig.getBatchsize());
        semaphore = new Semaphore(batchsize);
        cache = DataSamudayaCache.get();
        parallelExecutionPhaseDExecutorLocalMode(graph,
            new TaskProviderLocalMode(graph.vertexSet().size()));
      }
      // If mesos is scheduler run mesos framework.
      else if (Boolean.TRUE.equals(ismesos)) {
        MesosScheduler.runFramework(sptsl, graph,
            DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MESOS_MASTER), tasksptsthread,
            pipelineconfig.getJar());
      }
      // If Yarn is scheduler run yarn scheduler via spring yarn
      // framework.
      else if (Boolean.TRUE.equals(isyarn)) {
        yarnmutex.acquire();
        OutputStream os = pipelineconfig.getOutput();
        pipelineconfig.setOutput(null);
        new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
        Utils.createJar(new File(DataSamudayaConstants.YARNFOLDER), DataSamudayaConstants.LOCAL_FS_APPJRPATH,
            DataSamudayaConstants.YARNOUTJAR);
        var yarninputfolder =
            DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH + job.getId();
        RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(sptsl, yarninputfolder,
            DataSamudayaConstants.MASSIVEDATA_YARNINPUT_DATAFILE, pipelineconfig);
        RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(graph, yarninputfolder,
            DataSamudayaConstants.MASSIVEDATA_YARNINPUT_GRAPH_FILE, pipelineconfig);
        RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(tasksptsthread, yarninputfolder,
            DataSamudayaConstants.MASSIVEDATA_YARNINPUT_TASK_FILE, pipelineconfig);
        RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jsidjsmap, yarninputfolder,
            DataSamudayaConstants.MASSIVEDATA_YARNINPUT_JOBSTAGE_FILE, pipelineconfig);
        decideContainerCountAndPhysicalMemoryByBlockSize(sptsl.size(),
            Integer.parseInt(pipelineconfig.getBlocksize()));
        ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
            DataSamudayaConstants.FORWARD_SLASH + YarnSystemConstants.DEFAULT_CONTEXT_FILE_CLIENT,
            getClass());
        pipelineconfig.setOutput(os);
        var client = (CommandYarnClient) context.getBean(DataSamudayaConstants.YARN_CLIENT);
        if(nonNull(pipelineconfig.getJobname())) {
        	client.setAppName(pipelineconfig.getJobname());
        } else {
        	client.setAppName(DataSamudayaConstants.DATASAMUDAYA);
        }
        client.getEnvironment().put(DataSamudayaConstants.YARNDATASAMUDAYAJOBID, job.getId());
        var appid = client.submitApplication(true);
        var appreport = client.getApplicationReport(appid);
        yarnmutex.release();
        while (appreport.getYarnApplicationState() != YarnApplicationState.FINISHED
            && appreport.getYarnApplicationState() != YarnApplicationState.FAILED) {
          appreport = client.getApplicationReport(appid);
          Thread.sleep(1000);
        }
      }
      // If Jgroups is the scheduler;
      else if (Boolean.TRUE.equals(isjgroups)) {
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
          tasksgraphexecutor[stagegraphexecutorindex].setStorage(pipelineconfig.getStorage());
          taskgraphexecutormap.put(te, tasksgraphexecutor[stagegraphexecutorindex]);
          stagegraphexecutorindex++;
        }
        stagegraphexecutorindex = 0;
        Task tasktmp = null;
        var rand = new Random(System.currentTimeMillis());
        var taskhpmap = new ConcurrentHashMap<Task, String>();
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
        if(pipelineconfig.getUseglobaltaskexecutors()) {
        	jobid = pipelineconfig.getTejobid();
        }
        broadcastJobStageToTaskExecutors(tasks);
        for (var stagesgraphexecutor : tasksgraphexecutor) {
          if (!stagesgraphexecutor.getTasks().isEmpty()) {
            Utils.getResultObjectByInput(stagesgraphexecutor.getHostport(), stagesgraphexecutor, jobid);
          }
        }
        var stagepartids =
            tasks.parallelStream().map(taskpart -> taskpart.jobid + taskpart.taskid).collect(Collectors.toSet());
        var stagepartidstatusmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
        var stagepartidstatusmapreq = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
        try (var channel = Utils.getChannelTaskExecutor(jobid,
            NetworkUtil.getNetworkAddress(
                DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST)),
            Integer
                .parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT)),
            stagepartidstatusmapreq, stagepartidstatusmapresp);) {
          var totaltasks = tasks.size();
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
              log.debug("Total Percentage Completed: "
                  + Math.floor((totalcompleted / totaltasks) * 100.0));
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
      }
      // If not yarn or mesos schedule via standalone task executors
      // daemon.
      else {
        broadcastJobStageToTaskExecutors(new ArrayList<>(taskgraph.vertexSet()));
        graph = (SimpleDirectedGraph<StreamPipelineTaskSubmitter, DAGEdge>) parallelExecutionPhaseDExecutor(graph);
      }

      if(job.getJobtype() == JOBTYPE.PIG && job.getTrigger()!=TRIGGER.PIGDUMP) {
    	  return getFinalPhasesWithNoSuccessors(taskgraph);
      }
      // Obtain the final stage job results after final stage is
      // completed.
      List finalstageoutput = new ArrayList<>();
      if(job.getTrigger() == TRIGGER.FOREACH) {
    	  finalstageoutput = new ArrayList<>(sptss);
      } else {
    	  finalstageoutput = getLastStageOutput(sptss, graph, sptsl, ismesos, isyarn, islocal,
          isjgroups, resultstream);
      }
      if (Boolean.TRUE.equals(isjgroups)) {
        closeResourcesTaskExecutor(tasksgraphexecutor);
      }
      job.setIscompleted(true);
      job.getJm().setJobcompletiontime(System.currentTimeMillis());
      Utils.writeToOstream(pipelineconfig.getOutput(),
          "\nConcluded job in "
              + ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0)
              + " seconds");
      log.info("Concluded job in "
          + ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0)
          + " seconds");
      job.getJm().setTotaltimetaken(
          (job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0);
      Utils.writeToOstream(pipelineconfig.getOutput(), "Job stats " + job.getJm());
      log.info("Job stats " + job.getJm());
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
      if (!Objects.isNull(job.getIgcache())) {
        job.getIgcache().close();
      }
      if ((Boolean.FALSE.equals(ismesos) && Boolean.FALSE.equals(isyarn)
          && Boolean.FALSE.equals(islocal) || Boolean.TRUE.equals(isjgroups)) && !isignite) {
        if (!pipelineconfig.getUseglobaltaskexecutors()) {
          if (!Boolean.TRUE.equals(isjgroups)) {
            var cce = new FreeResourcesCompletedJob();
            cce.setJobid(job.getId());
            var tes=zo.getTaskExectorsByJobId(job.getId());
            for (var te : tes) {
              Utils.getResultObjectByInput(te, cce, job.getId());
            }
          }
          zo.deleteJob(job.getId());
          if(job.getTrigger() != TRIGGER.FOREACH && !job.getPipelineconfig().getIsremotescheduler()) {
        	  Utils.destroyTaskExecutors(job);
          }
        }
      }
      if(zo!=null) {
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
    }

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
			try (var baos = new ByteArrayOutputStream(); var output = new Output(baos)) {
				JobStage js = (JobStage) jsidjsmap.get(key);
				if(nonNull(js)) {
					js.setTejobid(finaljobid);
				}
				kryo.writeClassAndObject(output, js);
				output.flush();
				for (String te : jobexecutorsmap.get(key)) {
					Utils.getResultObjectByInput(te, baos.toByteArray(), finaljobid);
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
      if (nonNull(pipelineconfig.getCustomclasses())
          && !pipelineconfig.getCustomclasses().isEmpty()) {
        loadjar.setClasses(pipelineconfig.getCustomclasses().stream().map(clz -> clz.getName())
            .collect(Collectors.toCollection(LinkedHashSet::new)));
      }
      for (var lc : job.getLcs()) {
        List<Integer> ports = null;
        if (pipelineconfig.getUseglobaltaskexecutors()) {
          ports = lc.getCla().getCr().stream().map(cr -> {
            return cr.getPort();
          }).collect(Collectors.toList());
        } else {
          ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc, DataSamudayaConstants.EMPTY);
        }
        int index = 0;
        String tehost = lc.getNodehostport().split("_")[0];
        while (index < ports.size()) {
          while (true) {
            try (Socket sock = new Socket(tehost, ports.get(index))) {
              break;
            } catch (Exception ex) {
              Thread.sleep(200);
            }
          }
          if (nonNull(loadjar.getMrjar())) {
        	  log.debug("{}", Utils.getResultObjectByInput(
                tehost + DataSamudayaConstants.UNDERSCORE + ports.get(index), loadjar, pipelineconfig.getUseglobaltaskexecutors()?pipelineconfig.getTejobid():job.getId()));
          }
          index++;
        }
      }
      String jobid = job.getId();
      if(pipelineconfig.getUseglobaltaskexecutors()) {
    	  jobid = pipelineconfig.getTejobid();
      }
      var tes = zo.getTaskExectorsByJobId(jobid);
      taskexecutors = new LinkedHashSet<>(tes);
      while (taskexecutors.size() != job.getTaskexecutors().size()) {
        Thread.sleep(500);
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
   * Closes Stages in task executor making room for another tasks to be executed in task executors
   * JVM.
   * 
   * @param tasksgraphexecutor
   * @throws Exception
   */
  private void closeResourcesTaskExecutor(TasksGraphExecutor[] tasksgraphexecutor)
			throws Exception {
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void parallelExecutionPhaseDExecutorLocalMode(
      Graph<StreamPipelineTaskSubmitter, DAGEdge> graph, TaskProvider taskprovider)
      throws Exception {
    var es = newExecutor(batchsize);
    try {

      var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es,
          taskprovider);
      var executor =
          new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(config);
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void parallelExecutionPhaseIgnite(Graph<StreamPipelineTaskSubmitter, DAGEdge> graph,
      TaskProvider taskprovider) throws Exception {
    ExecutorService es = null;
    try {
      es = newExecutor(batchsize);
      var config = new DexecutorConfig<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(es,
          taskprovider);
      var executor =
          new DefaultDexecutor<StreamPipelineTaskSubmitter, StreamPipelineTaskExecutor>(config);
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
  public Graph<StreamPipelineTaskSubmitter, DAGEdge> parallelExecutionPhaseDExecutor(final Graph<StreamPipelineTaskSubmitter, DAGEdge> origgraph)
      throws Exception {
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
            servertotaltasks.put(spts.getHostPort(),
                servertotaltasks.get(spts.getHostPort()) + 1);
          }
        }
        var executionresultscomplete = dexecutor.execute(ExecutionConfig.NON_TERMINATING);
        erroredresult = executionresultscomplete.getErrored();        
        if (erroredresult.isEmpty() && !isErrored(lineagegraph.vertexSet())) {
          completed = true;
        } else {
          List<String> currentcontainers = null;
          if(pipelineconfig.getUseglobaltaskexecutors()) {
        	  currentcontainers = zo.getTaskExectorsByJobId(pipelineconfig.getTejobid());
          } else {
        	  currentcontainers = zo.getTaskExectorsByJobId(job.getId());
          }
          var containersfailed = new ArrayList<>(this.taskexecutors);
          containersfailed.removeAll(currentcontainers);
          lineagegraph = generateLineageGraph(lineagegraph,containersfailed, currentcontainers);
          log.info("Tasks Errored: Original Graph {} \nGenerating Lineage Graph: {}", origgraph, lineagegraph);
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
  
  public boolean isErrored(Set<StreamPipelineTaskSubmitter> spts) {
	  Optional<StreamPipelineTaskSubmitter> optionalspts = 
	  spts.stream().parallel().filter(spt->!spt.isCompletedexecution()).findFirst();
	  if(optionalspts.isPresent()) {
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
   * @param graphToProcess
   * @param downNodeIPs
   * @param availableNodes
   * @return lineage graph
   */
	public Graph<StreamPipelineTaskSubmitter, DAGEdge> generateLineageGraph(
			Graph<StreamPipelineTaskSubmitter, DAGEdge> graphToProcess, List<String> downNodeIPs,
			List<String> availableNodes) {		
		Graph<StreamPipelineTaskSubmitter, DAGEdge> lineagegraph = new SimpleDirectedGraph<>(DAGEdge.class);
		TopologicalOrderIterator<StreamPipelineTaskSubmitter, DAGEdge> sort = new TopologicalOrderIterator<>(
				graphToProcess);
		Map<String, StreamPipelineTaskSubmitter> map = new HashMap<>();
		// update nodes with available nodes
		while (sort.hasNext()) {
			StreamPipelineTaskSubmitter spts = sort.next();			
			// update node with available node IP
			Set<DAGEdge> defaultEdge = graphToProcess.incomingEdgesOf(spts);
			StreamPipelineTaskSubmitter updatedNode = new StreamPipelineTaskSubmitter(spts.getTask(),
					spts.getHostPort(), job.getPipelineconfig());
			updatedNode.setCompletedexecution(spts.isCompletedexecution());
			if (defaultEdge.size() > 0) {
				if (downNodeIPs.contains(spts.getHostPort())) {
					DAGEdge edg = defaultEdge.iterator().next();
					StreamPipelineTaskSubmitter sourceNode = graphToProcess.getEdgeSource(edg);
					StreamPipelineTaskSubmitter source = map.get(sourceNode.getTask().taskid);
					updatedNode.getTask().hostport = source.getHostPort();
					updatedNode.setHostPort(source.getHostPort());
					updatedNode.setCompletedexecution(false);
				}
			} else {
				if (downNodeIPs.contains(spts.getHostPort())) {
					String availableNodeIP = getAvailableTaskExecutor(spts.getTask(), availableNodes);
					updatedNode.getTask().hostport = availableNodeIP;
					updatedNode.setHostPort(availableNodeIP);
					updatedNode.setCompletedexecution(false);
				}
			}
			lineagegraph.addVertex(updatedNode);
			map.put(updatedNode.getTask().taskid, updatedNode);
			for (DAGEdge edge : defaultEdge) {
				StreamPipelineTaskSubmitter sourceNode = graphToProcess.getEdgeSource(edge);
				if (map.containsKey(sourceNode.getTask().taskid)) {
					lineagegraph.addEdge(map.get(sourceNode.getTask().taskid), updatedNode);
					reConfigureParentRDFForStageExecution(updatedNode, map.get(sourceNode.getTask().taskid));
				}
			}
		}
		return lineagegraph;
	}
	Random rand = new Random(System.currentTimeMillis());
	public String getAvailableTaskExecutor(Task task, List<String> availableNodes) {
		Object[] inputs = task.getInput();
		if(nonNull(inputs)) {
			for(Object obj: inputs) {
				if(obj instanceof BlocksLocation bl) {
					Block[] bls = bl.getBlock();
					Map<String, Set<String>> dnxref = bls[0].getDnxref();
					Optional<String> dnxrefopt = availableNodes.stream().parallel().map(node-> node.split(DataSamudayaConstants.UNDERSCORE)[0])
					.map(ip->dnxref.get(ip)).filter(dnaddrs->nonNull(dnaddrs))
					.flatMap(dnaddrs->dnaddrs.stream()).findFirst();
					if(dnxrefopt.isPresent()) {
						String dn = dnxrefopt.get();
						bls[0].setHp(dn);
						Optional<String> availablenode = availableNodes.stream().parallel()
						.filter(node->dn.startsWith(node.split(DataSamudayaConstants.UNDERSCORE)[0])).findFirst();
						if(availablenode.isPresent()) {
							return availablenode.get();
						}
						else {
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
		for (var rdf : prdf) {
			if (!Objects.isNull(rdf) && rdf.getTaskid().equals(pred.getTask().getTaskid())) {
				rdf.setHp(pred.getHostPort());
				break;
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
            if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
            	sptel = new StreamPipelineTaskExecutorLocalSQL(
                        jsidjsmap.get(task.jobid + task.stageid), resultstream, cache);
            } else {
            	sptel = new StreamPipelineTaskExecutorLocal(
                jsidjsmap.get(task.jobid + task.stageid), resultstream, cache);
            }
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
            Utils.writeToOstream(pipelineconfig.getOutput(), "\nPercentage Completed "
                + Math.floor((counttaskscomp / totaltasks) * 100.0) + "% \n");
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
          if(pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
        	  mdste = new StreamPipelineTaskExecutorIgniteSQL(jsidjsmap.get(task.jobid + task.stageid), task);
        	  mdste.setHdfspath(hdfsfilepath);
          } else {
        	  mdste = new StreamPipelineTaskExecutorIgnite(jsidjsmap.get(task.jobid + task.stageid), task);
          }
          try {
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
    org.slf4j.Logger log = LoggerFactory.getLogger(DAGScheduler.class);
    double totaltasks;
    double counttaskscomp = 0;
    double counttasksfailed = 0;
    Map<String, Semaphore> semaphores;
    AtomicBoolean shouldcontinueprocessing;
    
    public DAGScheduler(double totaltasks, Map<String, Semaphore> semaphores, AtomicBoolean shouldcontinueprocessing) {
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
				if (!spts.isCompletedexecution() && shouldcontinueprocessing.get()) {
					try {
						semaphores.get(spts.getTask().getHostport()).acquire();
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
						printresult.release();
						if(result.taskstatus == TaskStatus.FAILED) {
							spts.getTask().setTaskstatus(TaskStatus.FAILED);
							spts.getTask().stagefailuremessage = result.stagefailuremessage;
							spts.setCompletedexecution(false);
							shouldcontinueprocessing.set(false);
							throw new IllegalArgumentException("Task Failed");
						} else if(result.taskstatus == TaskStatus.COMPLETED) {
							spts.setCompletedexecution(true);
						}
						spts.getTask().setPiguuid(result.getPiguuid());
						return true;
					} catch (IllegalArgumentException e) {
						throw e;
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
						spts.setCompletedexecution(false);
					} finally {
						semaphores.get(spts.getTask().getHostport()).release();
					}
				}
				return spts.isCompletedexecution();

			}
      };
    }

  }

  /**
   * Calculate the container count via number of processors and container memory
   * 
   * @param blocksize
   * @param stagecount
   */
  private void decideContainerCountAndPhysicalMemoryByBlockSize(int stagecount, int blocksize) {
    com.sun.management.OperatingSystemMXBean os =
        (com.sun.management.OperatingSystemMXBean) java.lang.management.ManagementFactory
            .getOperatingSystemMXBean();
    var availablememorysize = os.getFreePhysicalMemorySize();
    var processors = os.getAvailableProcessors();
    blocksize = blocksize * 1024 * 1024;
    availablememorysize = (availablememorysize - blocksize) / processors;
    availablememorysize = availablememorysize / (1024 * 1024);
    System.setProperty("jobcount", "" + stagecount);
    System.setProperty("containercount", "" + processors);
    System.setProperty("containermemory", "" + availablememorysize);
  }

  private Map<String, StreamPipelineTaskSubmitter> tasksptsthread = new ConcurrentHashMap<>();

  /**
   * Form a graph for physical execution plan for obtaining the topological ordering of physical
   * execution plan.
   * 
   * @param currentstage
   * @param nextstage
   * @param stageoutputs
   * @param jobid
   * @param graph
   * @throws Exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
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
        for (var parentthread1 : outputparent2) {
          for (var parentthread2 : outputparent1) {
            partitionindex++;
            StreamPipelineTaskSubmitter spts;
            if ((parentthread1 instanceof BlocksLocation)
                && (parentthread2 instanceof BlocksLocation)) {
              spts = getPipelineTasks(jobid, Arrays.asList(parentthread1, parentthread2),
                  currentstage, partitionindex, currentstage.number, null);
            } else {
              spts = getPipelineTasks(jobid, null, currentstage, partitionindex,
                  currentstage.number, Arrays.asList(parentthread2, parentthread1));
            }
            tasksptsthread.put(
                spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
                spts);
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
      // Form the graph for union function.
      else if (function instanceof UnionFunction) {
        if (outputparent1.size() != outputparent2.size()) {
          throw new Exception("Partition Not Equal");
        }
        for (var inputparent1 : outputparent1) {
          for (var inputparent2 : outputparent2) {
            partitionindex++;
            var spts = getPipelineTasks(jobid, null, currentstage, partitionindex,
                currentstage.number, Arrays.asList(inputparent1, inputparent2));
            tasksptsthread.put(
                spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
                spts);
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
      // Form the edges and nodes for the JoinPair function.
      else if (function instanceof JoinPredicate || function instanceof LeftOuterJoinPredicate
          || function instanceof RightOuterJoinPredicate || function instanceof Join
          || function instanceof LeftJoin || function instanceof RightJoin) {
        for (var inputparent1 : outputparent1) {
          for (var inputparent2 : outputparent2) {
            partitionindex++;
            var spts = getPipelineTasks(jobid, null, currentstage, partitionindex,
                currentstage.number, Arrays.asList(inputparent1, inputparent2));
            tasksptsthread.put(
                spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
                spts);
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
            } else if (inputparent1 instanceof Task task) {
    			taskgraph.addVertex(task);
    			taskgraph.addVertex(spts.getTask());
    			taskgraph.addEdge(task, spts.getTask());
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
            } else if (inputparent2 instanceof Task task) {
    			taskgraph.addVertex(task);
    			taskgraph.addVertex(spts.getTask());
    			taskgraph.addEdge(task, spts.getTask());
    		}
          }
        }
      }
      // Form the nodes and edges for coalesce function.
		else if ((function instanceof Coalesce coalesce)) {
			var partkeys = Iterables.partition(outputparent1, (outputparent1.size()) / coalesce.getCoalescepartition())
					.iterator();
			if (islocal || isignite || isyarn || ismesos) {
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
				for (; partkeys.hasNext();) {
					var parentpartitioned = (List<StreamPipelineTaskSubmitter>) partkeys.next();

					Map<String, List<StreamPipelineTaskSubmitter>> hpspts = parentpartitioned.stream()
							.collect(Collectors.groupingBy(sptsmap -> sptsmap.getHostPort(), HashMap::new,
									Collectors.mapping(sptsmap -> sptsmap, Collectors.toList())));
					List<StreamPipelineTaskSubmitter> sptsl = new ArrayList<>();
					for (var entry : hpspts.entrySet()) {
						partitionindex++;
						var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
								(List) entry.getValue());
						graph.addVertex(spts);
						taskgraph.addVertex(spts.getTask());
						tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
						for (var input : entry.getValue()) {
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
						sptsl.add(spts);						
					}
					partitionindex++;
					var spts = getPipelineTasks(jobid, null, currentstage, partitionindex, currentstage.number,
							(List) sptsl);
					tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
					tasks.add(spts);
					graph.addVertex(spts);
					taskgraph.addVertex(spts.getTask());
					for (var input : sptsl) {
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
		} else if (function instanceof AggregateReduceFunction) {
        partitionindex++;
        var spts = getPipelineTasks(jobid, null, currentstage, partitionindex,
            currentstage.number, new ArrayList<>(outputparent1));
        tasks.add(spts);
        graph.addVertex(spts);
        taskgraph.addVertex(spts.getTask());
        tasksptsthread.put(
            spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid, spts);
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
      } else if (function instanceof HashPartitioner || function instanceof GroupByFunction) {
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
				} else if(input instanceof StreamPipelineTaskSubmitter parentthread) {
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
        } else {
        // Form the nodes and edges for map stage.
        for (var input : outputparent1) {
          partitionindex++;
          StreamPipelineTaskSubmitter spts;
          if (input instanceof BlocksLocation) {
            spts = getPipelineTasks(jobid, input, currentstage, partitionindex,
                currentstage.number, null);
            tasksptsthread.put(
                spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
                spts);
            graph.addVertex(spts);
            taskgraph.addVertex(spts.getTask());
		} else if (input instanceof Task task) {
			spts = getPipelineTasks(jobid, task, currentstage, partitionindex, 
					currentstage.number, null);
			tasksptsthread.put(spts.getTask().jobid + spts.getTask().stageid + 
					spts.getTask().taskid, spts);
			graph.addVertex(spts);
			taskgraph.addVertex(task);
			taskgraph.addVertex(spts.getTask());
			taskgraph.addEdge(task, spts.getTask());
		} else {
            var parentthread = (StreamPipelineTaskSubmitter) input;
            spts = getPipelineTasks(jobid, null, currentstage, partitionindex,
                currentstage.number, Arrays.asList(parentthread));
            tasksptsthread.put(
                spts.getTask().jobid + spts.getTask().stageid + spts.getTask().taskid,
                spts);
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
  @SuppressWarnings({"unchecked", "rawtypes"})
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  public Set<StreamPipelineTaskSubmitter> getFinalPhasesWithNoPredecessors(Graph graph,
      List<StreamPipelineTaskSubmitter> sptsl) {
    return sptsl.stream().filter(spts -> Graphs.predecessorListOf(graph, spts).isEmpty())
        .collect(Collectors.toCollection(LinkedHashSet::new));
  }

  /**
   * Obtain the final stage output as when statuses for final stage been completed.
   * 
   * @param graph
   * @param sptsl
   * @param ismesos
   * @param isyarn
   * @return
   * @throws PipelineException
   */
  @SuppressWarnings({"rawtypes"})
  public List getLastStageOutput(Set<StreamPipelineTaskSubmitter> sptss, Graph graph,
      List<StreamPipelineTaskSubmitter> sptsl, Boolean ismesos, Boolean isyarn, Boolean islocal,
      Boolean isjgroups, ConcurrentMap<String, OutputStream> resultstream)
      throws PipelineException {
    log.debug("HDFS Path TO Retrieve Final Task Output: " + hdfsfilepath);

    try {
      log.debug("Final Stages: " + sptss);
      Utils.writeToOstream(pipelineconfig.getOutput(), "Final Stages: " + sptss);

      if (Boolean.TRUE.equals(isignite)) {
        int partition = 0;
        for (var spts : sptss) {
          job.getOutput().add(spts);
          if (job.isIsresultrequired()) {
            // Get final stage results from ignite
            writeResultsFromIgnite(spts.getTask(), partition++, stageoutput);
          }
        }
      } else if (Boolean.TRUE.equals(islocal)) {
        if (job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE) {
          for (var spts : sptss) {
            var key = getIntermediateResultFS(spts.getTask());
            try (var fsstream = resultstream.get(key);
                ByteBufferInputStream bbis =
                    new ByteBufferInputStream(((ByteBufferOutputStream) fsstream).get());
                var input = new Input(bbis);) {
              var obj = Utils.getKryo().readClassAndObject(input);;
              resultstream.remove(key);
              writeOutputToFile(stageoutput.size(), obj);
              stageoutput.add(obj);
            } catch (Exception ex) {
              log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
              throw ex;
            }
          }
        }
      } else if (Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn)) {
        int partition = 0;
        if (job.getTrigger() != job.getTrigger().SAVERESULTSTOFILE) {
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
        sptss = getFinalPhasesWithNoSuccessors(graph, new ArrayList<StreamPipelineTaskSubmitter>(graph.vertexSet()));
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
            if(pipelineconfig.getUseglobaltaskexecutors()) {
            	rdf.setTejobid(pipelineconfig.getTejobid());
            }
            boolean isJGroups = Boolean.parseBoolean(pipelineconfig.getJgroups());
            rdf.setMode(isJGroups ? DataSamudayaConstants.JGROUPS : DataSamudayaConstants.STANDALONE);
            RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
            try (var input = new Input(pipelineconfig.getStorage()==STORAGE.INMEMORY || pipelineconfig.getStorage()==STORAGE.COLUMNARSQL || isjgroups?new ByteArrayInputStream(rdf.getData()):new SnappyInputStream(new ByteArrayInputStream(rdf.getData())));) {
              var result = Utils.getKryo().readClassAndObject(input);;              
              if(job.getJobtype() == JOBTYPE.PIG) {
            	  PrintWriter out = new PrintWriter(pipelineconfig.getPigoutput());
            	  Utils.printTableOrError((List)result, out, JOBTYPE.PIG);
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
   * @param partcount
   * @param result
   * @throws PipelineException
   * @throws MalformedURLException
   */
  @SuppressWarnings("rawtypes")
  public void writeOutputToFile(int partcount, Object result)
      throws PipelineException, MalformedURLException {
    if (job.getTrigger() == job.getTrigger().SAVERESULTSTOFILE) {
      URL url = new URL(job.getUri());
      boolean isfolder = url.getProtocol().equals(DataSamudayaConstants.FILE);
      try (
          OutputStream fsdos = isfolder
              ? new FileOutputStream(url.getPath() + DataSamudayaConstants.FORWARD_SLASH + job.getSavepath()
                  + DataSamudayaConstants.HYPHEN + partcount)
              : hdfs.create(new Path(
                  job.getUri().toString() + job.getSavepath() + DataSamudayaConstants.HYPHEN + partcount));
          BufferedWriter bw = new BufferedWriter(new OutputStreamWriter(fsdos))) {

        if (result instanceof List res) {
          for (var value : res) {
            bw.write(value.toString());
            bw.write(DataSamudayaConstants.NEWLINE);
          }
        } else {
          bw.write(result.toString());
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
   * @param spts
   * @param stageoutput
   * @throws PipelineException
   */
  @SuppressWarnings({"unchecked", "rawtypes"})
  public void writeOutputToFileInMemory(StreamPipelineTaskSubmitter spts, List stageoutput)
      throws PipelineException {
    try (var fsstream = getIntermediateInputStreamInMemory(spts.getTask());
        var input = new Input(fsstream);) {
      var obj = Utils.getKryo().readClassAndObject(input);;
      writeOutputToFile(stageoutput.size(), obj);
      stageoutput.add(obj);
    } catch (Exception ex) {
      log.error(PipelineConstants.JOBSCHEDULERFINALSTAGERESULTSERROR, ex);
      throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
    }
  }

  /**
   * Get the input streams from task object.
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
      if(pipelineconfig.getUseglobaltaskexecutors()) {
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
  @SuppressWarnings({"unchecked", "rawtypes"})
  private void writeOutputToHDFS(FileSystem hdfs, Task task, int partition, List stageoutput)
      throws Exception {
    try {
      var path = DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH
          + task.jobid + DataSamudayaConstants.FORWARD_SLASH + task.taskid;
      log.debug("Forming URL Final Stage:" + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
          + DataSamudayaConstants.FORWARD_SLASH + task.jobid + DataSamudayaConstants.FORWARD_SLASH + task.taskid);
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
      if(pipelineconfig.getUseglobaltaskexecutors()) {
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
   * @param task
   * @param partition
   * @param stageoutput
   * @throws Exception
   */
  @SuppressWarnings({"rawtypes", "unchecked"})
  private void writeResultsFromIgnite(Task task, int partition, List stageoutput) throws Exception {
    try {
      log.info("Eventual outcome Ignite task: " + task);

      try (
          var sis = new ByteArrayInputStream(
              job.getIgcache().get(task.jobid + task.stageid + task.taskid));
          var input = new Input(sis);) {
        var obj = Utils.getKryo().readClassAndObject(input);;
        if (!Objects.isNull(job.getUri())) {
          job.setTrigger(job.getTrigger().SAVERESULTSTOFILE);
          writeOutputToFile(partition, obj);
        } if(job.getJobtype() == JOBTYPE.PIG) {
      	  PrintWriter out = new PrintWriter(pipelineconfig.getPigoutput());
      	  Utils.printTableOrError((List)obj, out, JOBTYPE.PIG);
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
  private StreamPipelineTaskSubmitter getPipelineTasks(String jobid, Object input, Stage stage,
      int partitionindex, int currentstage, List<Object> parentthreads) throws PipelineException {
    try {
      var task = new Task();
      task.setTaskid(DataSamudayaConstants.TASK+DataSamudayaConstants.HYPHEN+job.getTaskidgenerator().getAndIncrement());
      task.jobid = jobid;
      task.stageid = stage.id;
      task.storage = pipelineconfig.getStorage();
      String hp = null;
      task.hbphysicaladdress = hbphysicaladdress;
      if (currentstage == 0 || parentthreads == null) {
    	task.parentremotedatafetch = null;
        if (input instanceof List inputl) {
          task.input = inputl.toArray();
          hp = ((BlocksLocation) task.input[0]).getExecutorhp();
        } else if (input instanceof BlocksLocation bl) {
          hp = bl.getExecutorhp();
          task.input = new Object[] {input};
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
              if(pipelineconfig.getUseglobaltaskexecutors()) {
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
      zo.createTasksForJobNode(jobid, task, (event)->{
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
