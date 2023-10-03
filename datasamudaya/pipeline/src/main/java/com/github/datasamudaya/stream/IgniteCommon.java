package com.github.datasamudaya.stream;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.io.Writer;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.IgniteState;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.multicast.TcpDiscoveryMulticastIpFinder;
import org.apache.log4j.Logger;
import org.jgrapht.Graph;
import org.jgrapht.GraphPath;
import org.jgrapht.alg.shortestpath.AllDirectedPaths;
import org.jgrapht.graph.DirectedAcyclicGraph;
import org.jgrapht.io.ComponentNameProvider;
import org.jgrapht.io.DOTExporter;
import org.jgrapht.io.ExportException;
import org.jgrapht.io.GraphExporter;
import org.jgrapht.traverse.TopologicalOrderIterator;

import com.github.datasamudaya.common.DAGEdge;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FileSystemSupport;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.functions.AggregateFunction;
import com.github.datasamudaya.common.functions.AggregateReduceFunction;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.GroupByKeyFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.scheduler.StreamJobScheduler;
import com.github.datasamudaya.stream.utils.FileBlocksPartitioner;
import com.github.datasamudaya.stream.utils.FileBlocksPartitionerHDFS;

/**
 * 
 * @author arun
 * The class MassiveDataIgniteCommon is derived from AbstractPipeline 
 * used for IgnitePipeline.
 */
public sealed class IgniteCommon extends AbstractPipeline permits IgnitePipeline,MapPairIgnite{
	private static Logger log = Logger.getLogger(IgniteCommon.class);
	public Job job = null;
	protected String protocol;
	protected DirectedAcyclicGraph<AbstractPipeline, DAGEdge> graph = new DirectedAcyclicGraph<>(DAGEdge.class);
	IntSupplier supplier;
	public PipelineConfig pipelineconfig;
	protected int blocksize;
	protected String folder;

	/**
	 * The function cacheInternal creates job object and
	 * assign the isresults value to job object. 
	 * @param isresults
	 * @return Job object.
	 * @throws PipelineException
	 */
	protected Job cacheInternal(boolean isresults,URI uri, String path) throws PipelineException  {
		try {
			var job = createJob();
			job.setIsresultrequired(isresults);
			if(!Objects.isNull(uri)) {
				job.setUri(uri.toString());
			}
			if(!Objects.isNull(path)) {
				job.setSavepath(path);
			}
			job.setResults(submitJob(job));
			return job;
		}
		catch(Exception ex) {
			log.error(PipelineConstants.CREATEOREXECUTEJOBERROR, ex);
			throw new PipelineException(PipelineConstants.CREATEOREXECUTEJOBERROR, (Exception)ex);
		}
	}

	/**
	 * Submit the job to job scheduler.
	 * @param job
	 * @return results of the job submitted.
	 * @throws Exception
	 */
	protected Object submitJob(Job job) throws Exception  {
		PipelineConfig pc = null;;
		if(root instanceof IgnitePipeline mdp) {
			pc = mdp.pipelineconfig;
		} else if(root instanceof MapPairIgnite mti) {
			pc = mti.pipelineconfig;
		}
		var js = new StreamJobScheduler();
		job.setPipelineconfig(pc);
		return js.schedule(job);

	}

	/**
	 * Create Job and get DAG
	 * @return Job object.
	 * @throws PipelineException
	 * @throws ExportException
	 * @throws IOException
	 * @throws URISyntaxException
	 */
	@SuppressWarnings("rawtypes")
	protected Job createJob() throws PipelineException, ExportException, IOException, URISyntaxException  {
		Job job;
		job = new Job();
		job.setId(DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID());
		job.setJm(new JobMetrics());
		job.getJm().setJobstarttime(System.currentTimeMillis());
		job.getJm().setJobid(job.getId());
		DataSamudayaJobMetrics.put(job.getJm());
		PipelineConfig pipelineconfig = null;
		if(root instanceof IgnitePipeline ip) {
			pipelineconfig = ip.pipelineconfig;
		}else if(root instanceof MapPairIgnite mpi) {
			pipelineconfig = mpi.pipelineconfig;
		}
		job.getJm().setMode(pipelineconfig.getMode());
		if(!this.mdsroots.isEmpty()) {
			for(AbstractPipeline root:this.mdsroots) {
				if(root instanceof IgnitePipeline mdpi && !Objects.isNull(mdpi.job)) {
					job.getInput().add(mdpi.job.getOutput());
				}
				else if(root instanceof MapPairIgnite mpi && !Objects.isNull(mpi.job)){
					job.getInput().add(mpi.job.getOutput());
				}
			}
		}else {
			this.job = job;
		}
		getDAG(job);
		return job;
	}
	int tmptaskid = 0;
	
	/**
	 * Form nodes and edges and get Directed Acyclic graph 
	 * @param root
	 * @param absfunction
	 */
	protected void formDAGAbstractFunction(AbstractPipeline root, Collection<AbstractPipeline> absfunction) {
		for (var func : absfunction) {			
			//Add the verted to graph. 
			graph.addVertex(func);
			//If root not null add edges between root and child nodes.
			if (root != null) {
				graph.addEdge(root, func);
			}
			//recursively form edges for root and child nodes.
			formDAGAbstractFunction(func, func.childs);
		}
	}
	private int stageid = 1;
	
	private String printTasks(List<AbstractPipeline> functions) {
		var tasksnames = functions.stream().map(absfunc->absfunc.task).collect(Collectors.toList());
		return tasksnames.toString();
	}
	private String printStages(Set<Stage> stages) {
		var stagenames = stages.stream().map(sta->sta.getId()).collect(Collectors.toList());
		return stagenames.toString();
	}
	private Set<Stage> finalstages = new LinkedHashSet<>();
	private Set<Stage> rootstages = new LinkedHashSet<>();
	Set<Object> finaltasks = new LinkedHashSet<>();
	
	/**
	 * Get Directed Acyclic graph for Pipeline APi from functions graph 
	 * to stages graph.
	 * @param job
	 * @throws PipelineException 
	 * @throws ExportException 
	 * @throws URISyntaxException 
	 * @throws IOException 
	 * @ 
	 */
	@SuppressWarnings({ "unchecked", "rawtypes" })
	protected void getDAG(Job job) throws PipelineException {
		try {
			log.debug("Induce of DAG started...");
			DirectedAcyclicGraph<Stage, DAGEdge> graphstages = null;
			Map<Object, Stage> taskstagemap = null;
			tmptaskid = 0;
			formDAGAbstractFunction(null, mdsroots);
			var absfunctions = graph.vertexSet();
			for (AbstractPipeline absfunction : absfunctions) {
				log.debug("\n\nTasks " + absfunction);
				log.debug("[Parent] [Child]");
				log.debug(printTasks(absfunction.parents) + " , " + printTasks(absfunction.childs));
				log.debug("Task");
				log.debug(PipelineUtils.getFunctions(absfunction.task));
			}
			taskstagemap = new HashMap<>();

			graphstages = new DirectedAcyclicGraph<>(DAGEdge.class);
			rootstages.clear();
			var topoaf = new TopologicalOrderIterator<>(graph);
			while (topoaf.hasNext()) {
				var af = topoaf.next();
				// If AbstractFunction is mds then create a new stage object
				// parent and
				// child stage and form the edge between stages.
				if ((af instanceof IgnitePipeline || af instanceof MapPairIgnite) && af.task instanceof Dummy) {
					var parentstage = new Stage();
					parentstage.setId(DataSamudayaConstants.STAGE+DataSamudayaConstants.HYPHEN+job.getStageidgenerator().getAndIncrement());
					rootstages.add(parentstage);
					graphstages.addVertex(parentstage);
					taskstagemap.put(af.task, parentstage);
				}
				// If abstract functions parent size is greater than 0 then
				// check if the first childs size is greater than or equal to 2.
				// Create new child stage and add abstract function to child and
				// form the edges
				// between parent and child.
				else if (af.parents.size() >= 2) {
					var childstage = new Stage();
					childstage.setId(DataSamudayaConstants.STAGE+DataSamudayaConstants.HYPHEN+job.getStageidgenerator().getAndIncrement());
					for (var afparent : af.parents) {
						var parentstage = taskstagemap.get(afparent.task);
						graphstages.addVertex(parentstage);
						graphstages.addVertex(childstage);
						graphstages.addEdge(parentstage, childstage);
						childstage.parent.add(parentstage);
						parentstage.child.add(childstage);
					}
					childstage.tasks.add(af.task);
					taskstagemap.put(af.task, childstage);
				}
				// If the abstract functions are ReduceFunction,
				// GroupByKeyFunction, JoinPairFunction,
				// JoinPairFunction, AggregateReduceFunction
				// SampleSupplierInteger, SampleSupplierPartition
				// UnionFunction, IntersectionFunction
				// and if the previous tasks is not added i.e no tasks
				// are added to stage then add it to tasks of the last available
				// stage.
				else if (af.parents.size() == 1) {
					// create a new stage and add the abstract function to
					// new stage created and form the edges between last stage
					// and new stage.
					// and pushed to stack.
					if (af.parents.get(0).childs.size() >= 2) {
						var childstage = new Stage();
						childstage.setId(DataSamudayaConstants.STAGE+DataSamudayaConstants.HYPHEN+job.getStageidgenerator().getAndIncrement());
						for (var afparent : af.parents) {
							Stage parentstage = taskstagemap.get(afparent.task);
							graphstages.addVertex(parentstage);
							graphstages.addVertex(childstage);
							graphstages.addEdge(parentstage, childstage);
							childstage.parent.add(parentstage);
							parentstage.child.add(childstage);
						}
						childstage.tasks.add(af.task);
						taskstagemap.put(af.task, childstage);
					} else if ((!Objects.isNull(af.task) && (af.task instanceof Coalesce
							|| af.task instanceof GroupByKeyFunction
							|| af.task instanceof HashPartitioner
							|| af.task instanceof GroupByFunction
							|| af.task instanceof CountByKeyFunction
							|| af.task instanceof CountByValueFunction
							|| af.task instanceof JoinPredicate
							|| af.task instanceof LeftOuterJoinPredicate
							|| af.task instanceof RightOuterJoinPredicate
							|| af.task instanceof AggregateFunction
							|| af.task instanceof AggregateReduceFunction
							|| af.task instanceof SampleSupplierInteger
							|| af.task instanceof SampleSupplierPartition
							|| af.task instanceof UnionFunction
							|| af.task instanceof FoldByKey
							|| af.task instanceof IntersectionFunction))) {
						stageCreator(graphstages, taskstagemap, af, job);
					} else if (! Objects.isNull(af.parents.get(0).task)
							&& !(af.parents.get(0).task instanceof Coalesce
									|| af.parents.get(0).task instanceof GroupByKeyFunction
									|| af.parents.get(0).task instanceof HashPartitioner
									|| af.parents.get(0).task instanceof GroupByFunction
									|| af.parents.get(0).task instanceof CountByKeyFunction
									|| af.parents.get(0).task instanceof CountByValueFunction
									|| af.parents.get(0).task instanceof JoinPredicate
									|| af.parents.get(0).task instanceof LeftOuterJoinPredicate
									|| af.parents.get(0).task instanceof RightOuterJoinPredicate
									|| af.parents.get(0).task instanceof AggregateFunction
									|| af.parents.get(0).task instanceof AggregateReduceFunction
									|| af.parents.get(0).task instanceof SampleSupplierInteger
									|| af.parents.get(0).task instanceof SampleSupplierPartition
									|| af.parents.get(0).task instanceof UnionFunction
									|| af.parents.get(0).task instanceof FoldByKey
									|| af.parents.get(0).task instanceof IntersectionFunction)) {
						var parentstage = taskstagemap.get(af.parents.get(0).task);
						parentstage.tasks.add(af.task);
						taskstagemap.put(af.task, parentstage);
					} else {
						stageCreator(graphstages, taskstagemap, af, job);
					}
				}
			}
			log.debug("Stages----------------------------------------");
			var stagesprocessed = graphstages.vertexSet();
			for (Stage stagetoprint : stagesprocessed) {
				log.debug("\n\nStage " + stagetoprint.getId());
				log.debug("[Parent] [Child]");
				log.debug(printStages(stagetoprint.parent) + " , " + printStages(stagetoprint.child));
				log.debug("Tasks");
				for (var task : stagetoprint.tasks) {
					log.debug(PipelineUtils.getFunctions(task));
				}
			}

			finalstages.clear();
			finalstages.add(taskstagemap.get(finaltasks.iterator().next()));
			
			// Directed paths

			var stages = new LinkedHashSet<>();
			if(rootstages.size() == 1 && finalstages.size() == 1 && rootstages.containsAll(finalstages)) {
				stages.addAll(rootstages);
			}
			else {
				// Directed paths
				var adp = new AllDirectedPaths<>(graphstages);
	
				// Get graph paths between root stage and final stage.
				List<GraphPath<Stage, DAGEdge>> graphPaths = adp.getAllPaths(rootstages, finalstages, true,
						Integer.MAX_VALUE);				
				// Collect the graph paths by getting source and target stages.
				for (var graphpath : graphPaths) {
					var dagedges = graphpath.getEdgeList();
					for (var dagedge : dagedges) {
						stages.add((Stage) dagedge.getSource());
						stages.add((Stage) dagedge.getTarget());
					}
				}
			}
			// Topological ordering of graph stages been computed so that
			// Stage of child will not be excuted not till all the parent stages
			// result been computed.
			Iterator<Stage> topostages = new TopologicalOrderIterator(graphstages);
			while (topostages.hasNext())
				job.getTopostages().add(topostages.next());
			job.getTopostages().retainAll(stages);
			if(job.getInput().isEmpty()) {
				if(protocol.equals(FileSystemSupport.HDFS)) {
					var fbpartitioner = new FileBlocksPartitionerHDFS();
					fbpartitioner.getJobStageBlocks(job, supplier, ((IgnitePipeline)root).protocol, rootstages, mdsroots, ((IgnitePipeline)root).blocksize, ((IgnitePipeline)root).pipelineconfig);
				}
				else if(protocol.equals(FileSystemSupport.FILE)) {
					var fbp = new FileBlocksPartitioner();
					fbp.getJobStageBlocks(job, ((IgnitePipeline)root).pipelineconfig, folder,mdsroots, rootstages);
				}
			} else {
				var cfg = new IgniteConfiguration();

				// The node will be started as a client node.
				cfg.setClientMode(true);
				cfg.setDeploymentMode(DeploymentMode.CONTINUOUS);
				// Classes of custom Java logic will be transferred over the wire from
				// this app.
				cfg.setPeerClassLoadingEnabled(true);
				// Setting up an IP Finder to ensure the client can locate the servers.
				var ipFinder = new TcpDiscoveryMulticastIpFinder();
				ipFinder.setAddresses(Arrays.asList(pipelineconfig.getIgnitehp()));
				cfg.setDiscoverySpi(new TcpDiscoverySpi().setIpFinder(ipFinder));
				var cc = new CacheConfiguration(DataSamudayaConstants.DATASAMUDAYACACHE);
				cc.setCacheMode(CacheMode.PARTITIONED);
				cc.setAtomicityMode(CacheAtomicityMode.ATOMIC);
				cc.setBackups(Integer.parseInt(pipelineconfig.getIgnitebackup()));
				cfg.setCacheConfiguration(cc);
				// Starting the node
				Ignite ignite = null; 
				if (IgniteState.STARTED == Ignition.state()) {
					// The default instance is already started, do not start another one
					ignite = Ignition.ignite();
				}
				else {					
					ignite = Ignition.start(cfg);				
				}
				IgniteCache<Object, byte[]> ignitecache = ignite.cache(DataSamudayaConstants.DATASAMUDAYACACHE);
				job.setIgnite(ignite);
				var computeservers = job.getIgnite().cluster().forServers();
				job.getJm().setContainersallocated(computeservers.hostNames().stream().collect(Collectors.toMap(key->key, value->0d)));
				job.setIgcache(ignitecache);
				job.setStageoutputmap(new ConcurrentHashMap<>());
				var inputstages = new ArrayList<Stage>();
				for(var stage:job.getTopostages()) {
					if(stage.tasks.isEmpty()) {
						inputstages.add(stage);
					}
				}
				if(inputstages.size()==1) {
					job.getStageoutputmap().put(job.getTopostages().get(0),job.getInput().get(0));
				}
				else {
					int index = 0;
					for(Stage stage:inputstages) {
						job.getStageoutputmap().put(stage,job.getInput().get(index));
						index++;
					}
				}
			}
			var writer = new StringWriter();
			if (Boolean.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
				Utils.renderGraphStage(graphstages, writer);
			}

			if (Boolean.parseBoolean((String) DataSamudayaProperties.get().get(DataSamudayaConstants.GRAPHSTOREENABLE))) {
				writer = new StringWriter();
				renderGraph(graph, writer);
			}

			stages.clear();
			stages = null;
			log.debug("Induce of DAG ended.");
		} catch (Exception ex) {
			log.error(PipelineConstants.DAGERROR,ex);
			throw new PipelineException(PipelineConstants.DAGERROR, ex);
		}
	}

	/**
	 * The method stageCreator creates stage object and forms graph nodes
	 * and edges
	 * @param graphstages
	 * @param taskstagemap
	 * @param af
	 */
	private void stageCreator(DirectedAcyclicGraph<Stage, DAGEdge> graphstages,
	Map<Object, Stage> taskstagemap,AbstractPipeline af, Job job) {
		var parentstage = taskstagemap.get(af.parents.get(0).task);
		var childstage = new Stage();
		childstage.setId(DataSamudayaConstants.STAGE+DataSamudayaConstants.HYPHEN+job.getStageidgenerator().getAndIncrement());
		childstage.tasks.add(af.task);
		graphstages.addVertex(parentstage);
		graphstages.addVertex(childstage);
		graphstages.addEdge(parentstage, childstage);
		childstage.parent.add(parentstage);
		parentstage.child.add(childstage);
		taskstagemap.put(af.task, childstage);
	}
	
	/**
	 * The method renderGraph writes the graph information to files.
	 * @param graph
	 * @param writer
	 * @throws ExportException
	 */
	private static void renderGraph(Graph<AbstractPipeline, DAGEdge> graph,Writer writer) throws ExportException  {
		ComponentNameProvider<AbstractPipeline> vertexIdProvider = task -> {
			
			try {
				Thread.sleep(500);
			} catch (Exception ex) {
				log.error("Delay Error, see cause below \n",ex);
			}
			return "" + System.currentTimeMillis();
		
	};
	ComponentNameProvider<AbstractPipeline> vertexLabelProvider = AbstractPipeline::toString;
	GraphExporter<AbstractPipeline, DAGEdge> exporter = new DOTExporter<>(vertexIdProvider, vertexLabelProvider, null);
	exporter.exportGraph(graph, writer);
	var path = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPDIRPATH);
	new File(path).mkdirs();
	try(var stagegraphfile = new FileWriter(path+DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GRAPHTASKFILENAME)+System.currentTimeMillis());) {
		stagegraphfile.write(writer.toString());
	} catch (Exception e) {
		log.error("File Write Error, see cause below \n",e);
	}
}
}
