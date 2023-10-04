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
package com.github.datasamudaya.tasks.scheduler;

import java.net.Socket;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.AllocateContainers;
import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.Block;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ContainerLaunchAttributes;
import com.github.datasamudaya.common.ContainerResources;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DestroyContainers;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.ReducerValues;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.RetrieveKeys;
import com.github.datasamudaya.common.Tuple3Serializable;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.ExecutionResult;
import com.github.dexecutor.core.task.ExecutionResults;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;
import com.google.common.collect.Iterables;

import static java.util.Objects.*;

/**
 * Map reduce application to execute map and reduce tasks.
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class MapReduceApplication implements Callable<List<DataCruncherContext>> {
	String jobname;
	JobConfiguration jobconf;
	protected List<MapperInput> mappers;
	protected List<Object> combiners;
	protected List<Object> reducers;
	String outputfolder;
	int batchsize;
	int numreducers;
	public Set<String> hdfsdirpath;
	FileSystem hdfs;
	List<Path> blockpath = new ArrayList<>();
	int totalreadsize;
	byte[] read1byt = new byte[1];
	int blocksize;
	Path currentfilepath;
	int blocklocationindex;
	long redcount;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = Logger.getLogger(MapReduceApplication.class);
	List<LocatedBlock> locatedBlocks;
	int executorindex;
	ExecutorService es;
	JobMetrics jm = new JobMetrics();
	Map<String, String> apptaskhp = new ConcurrentHashMap<>();
	public MapReduceApplication(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Object> combiners, List<Object> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}


	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;
	public List<LaunchContainers> lcs = new ArrayList<>();
	private ConcurrentMap<String, Resources> resources;

	private void getContainersBalanced(List<BlocksLocation> bls) throws MapReduceException {
		log.debug("Entered DataSamudayaJob.getContainersBalanced");
		var hostcontainermap = containers.stream()
				.collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.UNDERSCORE)[0],
						Collectors.mapping(container -> container,
								Collectors.toCollection(ArrayList::new))));
		var containerallocatecount = (Map<String, Long>) containers.stream().parallel().collect(Collectors.toMap(container -> container, container -> 0l));
		List<String> hostportcontainer;
		for (var b : bls) {
			hostportcontainer = hostcontainermap.get(b.getBlock()[0].getHp().split(DataSamudayaConstants.COLON)[0]);
			var optional = hostportcontainer.stream().sorted((xref1, xref2) -> {
				return containerallocatecount.get(xref1).compareTo(containerallocatecount.get(xref2));
			}).findFirst();
			if (optional.isPresent()) {
				var container = optional.get();
				b.setExecutorhp(container);
				containerallocatecount.put(container, containerallocatecount.get(container) + 1);
			} else {
				throw new MapReduceException(DataSamudayaConstants.CONTAINERALLOCATIONERROR);
			}
		}
		log.debug("Exiting DataSamudayaJob.getContainersBalanced");
	}

	Map<String,List<ContainerResources>> nodecrsmap = new ConcurrentHashMap<>();
	
	public void getTaskExecutors(List<BlocksLocation> bls, String appid, String applicationid) throws PipelineException {
		try {
			containers = new ArrayList<>();
			var totalcontainersallocated = 0;
			var nodestotalblockmem = new ConcurrentHashMap<String, Long>();
			getNodesResourcesSorted(bls, nodestotalblockmem);
			var resources = DataSamudayaNodesResources.get();
			for (var node : nodessorted) {
				var host = node.split("_")[0];
				var lc = new LaunchContainers();
				lc.setNodehostport(node);
				lc.setJobid(appid);
				lc.setAppid(appid);
				var cla = new ContainerLaunchAttributes();
				var cr =
						getNumberOfContainers(jobconf.getGctype(), nodestotalblockmem.get(host),
								resources.get(node));
				if (cr.isEmpty()) {
					continue;
				}
				lcs.add(lc);
				cla.setNumberofcontainers(cr.size());
				cla.setCr(cr);
				lc.setCla(cla);
				var ac = new AllocateContainers();
				ac.setJobid(appid);
				ac.setNumberofcontainers(cr.size());
				ports = (List<Integer>) Utils.getResultObjectByInput(node, ac, DataSamudayaConstants.EMPTY);
				Resources allocresources = resources.get(node);
				for (int containercount = 0; containercount < ports.size(); containercount++) {
					ContainerResources crs = cr.get(containercount);
					long maxmemory = crs.getMaxmemory();
					long directheap = crs.getDirectheap();
					allocresources.setFreememory(allocresources.getFreememory()-maxmemory-directheap);
					allocresources.setNumberofprocessors(allocresources.getNumberofprocessors()-crs.getCpu());
					crs.setPort(ports.get(containercount));
					containers.add(host + DataSamudayaConstants.UNDERSCORE + ports.get(containercount));
				}
				totalcontainersallocated += cr.size();
				nodecrsmap.put(node, cr);
			}
			jm.setContainerresources(lcs.stream().flatMap(lc -> {
				var crs = lc.getCla().getCr();
				return crs.stream().map(cr -> {
					var node = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
					var cpu = cr.getCpu();
					var maxmemory = cr.getMaxmemory();
					var directmemory = cr.getDirectheap();
					var port = cr.getPort();
					return DataSamudayaConstants.BR + node + DataSamudayaConstants.UNDERSCORE + port + DataSamudayaConstants.COLON + DataSamudayaConstants.BR + DataSamudayaConstants.CPUS
							+ DataSamudayaConstants.EQUAL + cpu + DataSamudayaConstants.BR + DataSamudayaConstants.MEM + DataSamudayaConstants.EQUAL
							+ maxmemory + DataSamudayaConstants.ROUNDED_BRACKET_OPEN + (Math.floor(maxmemory / (double) (maxmemory + directmemory) * 100.0))
							+ DataSamudayaConstants.ROUNDED_BRACKET_CLOSE + DataSamudayaConstants.BR + DataSamudayaConstants.DIRECTMEM + DataSamudayaConstants.EQUAL + directmemory
							+ DataSamudayaConstants.ROUNDED_BRACKET_OPEN + (Math.floor(directmemory / (double) (maxmemory + directmemory) * 100.0))
							+ DataSamudayaConstants.ROUNDED_BRACKET_CLOSE;

				}).collect(Collectors.toList()).stream();
			}).collect(Collectors.toList()));
			log.debug("Total Containers Allocated:"	+ totalcontainersallocated);
		} catch (Exception ex) {
			log.error(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
			throw new PipelineException(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
		}
	}

	public void getDnXref(List<BlocksLocation> bls, boolean issa) throws MapReduceException {
		log.debug("Entered DataSamudayaJob.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if (bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
				for (; xref.hasNext(); ) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));
		if (issa) {
			List<String> nodes = null;
			if(nonNull(jobconf.isIsuseglobalte()) && !jobconf.isIsuseglobalte() || isNull(jobconf.isIsuseglobalte())) {
				resources = DataSamudayaNodesResources.get();
				nodes = resources.keySet().stream().map(node -> node.split(DataSamudayaConstants.UNDERSCORE)[0])
						.collect(Collectors.toList());
			} else {
				nodes = nodessorted.stream().map(node -> node.split(DataSamudayaConstants.UNDERSCORE)[0])
						.collect(Collectors.toList());
			}
			final var computingnodes = new ArrayList<>(nodes); 
			for (var b : bls) {
				var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
						.filter(xrefhost -> computingnodes.contains(xrefhost))
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				if (xrefselected.isEmpty()) {
					throw new MapReduceException(
							PipelineConstants.INSUFFNODESERROR + " Available computing nodes are "
									+ computingnodes + " Available Data Nodes are " + b.getBlock()[0].getDnxref().keySet());
				}
				final var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[0].setHp(xref);
				if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
					xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
							.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream())
							.filter(xrefhp -> xrefhp.split(DataSamudayaConstants.COLON)[0]
									.equals(xref.split(DataSamudayaConstants.COLON)[0]))
							.findFirst();
					if (xrefselected.isEmpty()) {
						xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
								.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).findFirst();
						if (xrefselected.isEmpty()) {
							throw new MapReduceException(PipelineConstants.INSUFFNODESERROR
									+ " Available computing nodes are " + computingnodes + " Available Data Nodes are "
									+ b.getBlock()[1].getDnxref().keySet());
						}
					}
					var xref1 = xrefselected.get();
					b.getBlock()[1].setHp(xref1);
				}
			}
		} else {
			for (var b : bls) {
				var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[0].setHp(xref);
				if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
					xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
							.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
						return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
					}).findFirst();
					xref = xrefselected.get();
					b.getBlock()[1].setHp(xref);
				}
			}
		}
		log.debug("Exiting DataSamudayaJob.getDnXref");
	}

	void getNodesResourcesSorted(List<BlocksLocation> bls, Map<String, Long> nodestotalblockmem) {
		resources = DataSamudayaNodesResources.get();

		var nodeswithhostonly = bls.stream().flatMap(bl -> {
			var block1 = bl.getBlock()[0];
			Block block2 = null;
			if (bl.getBlock().length > 1) {
				block2 = bl.getBlock()[1];
			}
			var xref = new HashSet<String>();
			if (!Objects.isNull(block1)) {
				xref.add(block1.getHp().split(DataSamudayaConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block1.getHp().split(DataSamudayaConstants.COLON)[0]);
				if (value != null) {
					nodestotalblockmem.put(block1.getHp().split(DataSamudayaConstants.COLON)[0], value + (block1.getBlockend() - block1.getBlockstart()));
				} else {
					nodestotalblockmem.put(block1.getHp().split(DataSamudayaConstants.COLON)[0], block1.getBlockend() - block1.getBlockstart());
				}
			}
			if (!Objects.isNull(block2)) {
				xref.add(block2.getHp().split(DataSamudayaConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block2.getHp().split(DataSamudayaConstants.COLON)[0]);
				if (value != null) {
					nodestotalblockmem.put(block2.getHp().split(DataSamudayaConstants.COLON)[0], value + (block2.getBlockend() - block2.getBlockstart()));
				} else {
					nodestotalblockmem.put(block2.getHp().split(DataSamudayaConstants.COLON)[0], block2.getBlockend() - block2.getBlockstart());
				}
			}
			return xref.stream();
		}).collect(Collectors.toSet());
		nodessorted = resources.entrySet().stream().sorted((entry1, entry2) -> {
			var r1 = entry1.getValue();
			var r2 = entry2.getValue();
			if (r1.getNumberofprocessors() < r2.getNumberofprocessors()) {
				return -1;
			} else if (r1.getNumberofprocessors() == r2.getNumberofprocessors()) {
				if (r1.getFreememory() < r2.getFreememory()) {
					return -1;
				} else if (r1.getFreememory() == r2.getFreememory()) {
					return 0;
				}
				else {
					return 1;
				}
			} else {
				return 1;
			}
		}).map(entry -> entry.getKey())
				.filter(key -> nodeswithhostonly.contains(key.split(DataSamudayaConstants.UNDERSCORE)[0]))
				.collect(Collectors.toList());
	}

	protected List<ContainerResources> getNumberOfContainers(String gctype, long totalmem, Resources resources)
			throws PipelineException {
		var cpu = resources.getNumberofprocessors() - 1;
		var cr = new ArrayList<ContainerResources>();
		if (jobconf.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT)) {
			var res = new ContainerResources();
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			res.setCpu(cpu);
			var memoryrequire = actualmemory;
			var heapmem = memoryrequire * Integer.valueOf(jobconf.getHeappercentage()) / 100;
			res.setMinmemory(heapmem);
			res.setMaxmemory(heapmem);
			res.setDirectheap(memoryrequire - heapmem);
			res.setGctype(gctype);
			cr.add(res);
			return cr;
		} else if (jobconf.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_DIVIDED)) {
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var numofcontainerspermachine = Integer.parseInt(jobconf.getNumberofcontainers());
			var dividedcpus = cpu / numofcontainerspermachine;
			var maxmemory = actualmemory / numofcontainerspermachine;
			var numberofcontainer = 0;
			while (true) {
				if (cpu >= dividedcpus && actualmemory >= 0) {
					var res = new ContainerResources();
					res.setCpu(dividedcpus);
					var heapmem = maxmemory * Integer.valueOf(jobconf.getHeappercentage()) / 100;
					res.setMinmemory(maxmemory);
					res.setMaxmemory(maxmemory);
					res.setDirectheap(maxmemory - heapmem);
					res.setGctype(gctype);
					cpu -= dividedcpus;
					actualmemory -= maxmemory;
					cr.add(res);
				} else if (cpu >= 1 && actualmemory >= 0) {
					var res = new ContainerResources();
					res.setCpu(cpu);
					var heapmem = maxmemory * Integer.valueOf(jobconf.getHeappercentage()) / 100;
					res.setMinmemory(maxmemory);
					res.setMaxmemory(maxmemory);
					res.setDirectheap(maxmemory - heapmem);
					res.setGctype(gctype);
					cpu = 0;
					actualmemory -= maxmemory;
					cr.add(res);
				} else {
					break;
				}
				numberofcontainer++;
				if (numofcontainerspermachine == numberofcontainer) {
					break;
				}

			}
			return cr;
		} else if (jobconf.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_IMPLICIT)) {
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			var numberofimplicitcontainers = Integer.valueOf(jobconf.getImplicitcontainerallocanumber());
			var numberofimplicitcontainercpu = Integer.valueOf(jobconf.getImplicitcontainercpu());
			var numberofimplicitcontainermemory = jobconf.getImplicitcontainermemory();
			var numberofimplicitcontainermemorysize = Long.valueOf(jobconf.getImplicitcontainermemorysize());
			var memorysize = "GB".equals(numberofimplicitcontainermemory) ? DataSamudayaConstants.GB : DataSamudayaConstants.MB;
			var memoryrequired = numberofimplicitcontainermemorysize * memorysize;
			if (actualmemory < memoryrequired * numberofimplicitcontainers) {
				throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
			}
			if (cpu < numberofimplicitcontainercpu * numberofimplicitcontainers) {
				throw new PipelineException(PipelineConstants.INSUFFCPUALLOCATIONERROR);
			}
			for (var count = 0; count < numberofimplicitcontainers; count++) {
				var res = new ContainerResources();
				res.setCpu(numberofimplicitcontainercpu);
				var heapmem = memoryrequired * Integer.valueOf(jobconf.getHeappercentage())
						/ 100;
				res.setMinmemory(memoryrequired);
				res.setMaxmemory(memoryrequired);
				res.setDirectheap(memoryrequired - heapmem);
				res.setGctype(gctype);
				cr.add(res);
			}
			return cr;
		} else {
			throw new PipelineException(PipelineConstants.UNSUPPORTEDMEMORYALLOCATIONMODE);
		}
	}
	int containercount;

	public void getContainers(String appid) throws Exception {
		var loadjar = new LoadJar();
		loadjar.setMrjar(jobconf.getMrjar());
		List<String> containers = new ArrayList<>();
		for (var lc : lcs) {
			List<Integer> ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc, DataSamudayaConstants.EMPTY);
			int index = 0;
			String tehost = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
			while (index < ports.size()) {
				containers.add(tehost + DataSamudayaConstants.UNDERSCORE + ports.get(index));
				while (true) {
					try (Socket sock = new Socket(tehost, ports.get(index))) {
						break;
					} catch (Exception ex) {
						Thread.sleep(1000);
					}
				}
				if (!Objects.isNull(loadjar.getMrjar())) {
					log.info(Utils.getResultObjectByInput(tehost + DataSamudayaConstants.UNDERSCORE + ports.get(index), loadjar,
							appid));
				}
				index++;
			}
		}
		this.containers = containers;
	}
	boolean isexception;
	String exceptionmsg = DataSamudayaConstants.EMPTY;

	protected List<String> getHostPort(Collection<String> appidtaskids) {
		return appidtaskids.stream().map(apptaskid->apptaskhp.get(apptaskid))
				.collect(Collectors.toList());
	}
	
	@SuppressWarnings({"unchecked"})
	public List<DataCruncherContext> call() {
		var applicationid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueAppID();
		try {
			var starttime = System.currentTimeMillis();
			var containerscount = 0;
			es = Executors.newWorkStealingPool();
			cf = CuratorFrameworkFactory.newClient(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_HOSTPORT),
					20000, 50000, new RetryForever(
							Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_RETRYDELAY))));
			cf.start();

			jm.setJobstarttime(System.currentTimeMillis());
			var isblocksuserdefined = Boolean.parseBoolean(jobconf.getIsblocksuserdefined());			
			jm.setJobid(applicationid);
			DataSamudayaJobMetrics.put(jm);
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			blocksize = Integer.parseInt(jobconf.getBlocksize());
			hdfs = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);

			var combiner = new HashSet<Object>();
			var reducer = new HashSet<Object>();
			var mapclz = new HashMap<String, Set<Object>>();
			hdfsdirpath = new LinkedHashSet<>();
			for (var mapperinput : mappers) {
				try {
					if (mapclz.get(mapperinput.inputfolderpath) == null) {
						mapclz.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclz.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper);
					hdfsdirpath.add(mapperinput.inputfolderpath);
				} catch (Error ex) {

				}
			}
			Map<String, List<TaskSchedulerMapperCombinerSubmitter>> containermappercombinermap = new ConcurrentHashMap<>();
			var mrtaskcount = 0;
			var folderblocks = new ConcurrentHashMap<String, List<BlocksLocation>>();
			var allfilebls = new ArrayList<BlocksLocation>();
			var allfiles = new ArrayList<String>();
			var fileStatuses = new ArrayList<FileStatus>();
			for (var hdfsdir : hdfsdirpath) {
				var fileStatus = hdfs.listFiles(
						new Path(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + hdfsdir), true);
				while (fileStatus.hasNext()) {
					fileStatuses.add(fileStatus.next());
				}
				var paths = FileUtil.stat2Paths(fileStatuses.toArray(new FileStatus[fileStatuses.size()]));
				blockpath.addAll(Arrays.asList(paths));
				bls = new ArrayList<>();
				bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, null));				
				folderblocks.put(hdfsdir, bls);
				allfilebls.addAll(bls);
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.setTotalfilesize(jm.getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, blockpath));
				blockpath.clear();
				fileStatuses.clear();
			}
			
			if(nonNull(jobconf.isIsuseglobalte()) && jobconf.isIsuseglobalte()) {
				var lcs = GlobalContainerLaunchers.get(jobconf.getUser());
				containers = lcs.stream().flatMap(lc -> {
					var host = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE);
					return lc.getCla().getCr().stream().map(cr -> {
						return host[0] + DataSamudayaConstants.UNDERSCORE + cr.getPort();
					}).collect(Collectors.toList()).stream();
				}).collect(Collectors.toList());
				nodessorted = lcs.stream().map(lc -> lc.getNodehostport()).collect(Collectors.toList());
				getDnXref(allfilebls, true);
				getContainersBalanced(allfilebls);
			} else {
				getDnXref(allfilebls, true);
				getTaskExecutors(allfilebls, applicationid, applicationid);
				getContainersBalanced(allfilebls);
				getContainers(applicationid);
			}			
			containerscount = containers.size();

			jm.setTotalfilesize(jm.getTotalfilesize() / DataSamudayaConstants.MB);
			jm.setFiles(allfiles);
			jm.setNodes(new LinkedHashSet<>(nodessorted));
			jm.setContainersallocated(containers.stream().collect(Collectors.toMap(key -> key, value -> 0d)));
			;
			jm.setMode(jobconf.getExecmode());
			jm.setTotalblocks(allfilebls.size());
			for (var obj : combiners) {
				if (obj != null) {
					combiner.add(obj);
				}
			}
			for (var obj : reducers) {
				if (obj != null) {
					reducer.add(obj);
				}
			}			
			for (var folder : hdfsdirpath) {
				var mapclznames = mapclz.get(folder);
				var bls = folderblocks.get(folder);
				for (var bl : bls) {
					var taskid = DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + (mrtaskcount + 1);
					var apptask = new ApplicationTask();
					apptask.setApplicationid(applicationid);
					apptask.setTaskid(taskid);
					var mdtstm = (TaskSchedulerMapperCombinerSubmitter) getMassiveDataTaskSchedulerThreadMapperCombiner(
							mapclznames, combiner, bl, apptask, applicationid);
					if (containermappercombinermap.get(mdtstm.getHostPort()) == null) {
						containermappercombinermap.put(mdtstm.getHostPort(), new ArrayList<>());
					}
					containermappercombinermap.get(mdtstm.getHostPort()).add(mdtstm);
					apptaskhp.put(applicationid + taskid, mdtstm.getHostPort());
					mrtaskcount++;
				}

			}
			log.debug("Total MapReduce Tasks: " + mrtaskcount);
			var dccmapphases = new ArrayList<DataCruncherContext>();
			var completed = false;
			var numexecute = 0;
			var taskexeccount = Integer.parseInt(jobconf.getTaskexeccount());
			List<ExecutionResults<TaskSchedulerMapperCombinerSubmitter, Boolean>> erroredresult = new ArrayList<>();
			
			var semaphores = new ConcurrentHashMap<String, Semaphore>();
			if(nonNull(jobconf.isIsuseglobalte()) && !jobconf.isIsuseglobalte() || isNull(jobconf.isIsuseglobalte())) {
				for(var node:nodessorted) {
					var crs = nodecrsmap.get(node);
					String[] nodehp = node.split(DataSamudayaConstants.UNDERSCORE);
					for(ContainerResources cr: crs) {
						batchsize += cr.getCpu()*2;
						semaphores.put(nodehp[0]+DataSamudayaConstants.UNDERSCORE+cr.getPort(), new Semaphore(cr.getCpu()*2));
					}
				}
			} else {
				var lcs = GlobalContainerLaunchers.get(jobconf.getUser());
				lcs.stream().forEach(lc->{
					String[] nodehp = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE);
					for(ContainerResources cr: lc.getCla().getCr()) {
						batchsize += cr.getCpu()*2;
						semaphores.put(nodehp[0]+DataSamudayaConstants.UNDERSCORE+cr.getPort(), new Semaphore(cr.getCpu()*2));
					}
				});
			}
			while (!completed && numexecute < taskexeccount) {
				for (var containerkey : containermappercombinermap.keySet()) {
					var dccmapphase = new DataCruncherContext();
					dccmapphases.add(dccmapphase);
					List<TaskSchedulerMapperCombinerSubmitter> tasks = containermappercombinermap.get(containerkey);
					DexecutorConfig<TaskSchedulerMapperCombinerSubmitter, Boolean> configexec = new DexecutorConfig(
							newExecutor(),
							new MapCombinerTaskExecutor(semaphores.get(containerkey), dccmapphase, tasks.size()));
					DefaultDexecutor<TaskSchedulerMapperCombinerSubmitter, Boolean> dexecutor = new DefaultDexecutor<>(
							configexec);
					tasks.stream().forEach(task -> dexecutor.addIndependent(task));
					
					erroredresult.add(dexecutor.execute(ExecutionConfig.NON_TERMINATING));
				}
				completed = true;
				for (ExecutionResults<TaskSchedulerMapperCombinerSubmitter, Boolean> execs : erroredresult) {
					if(execs.getErrored().size()>0) {
						completed = false;
					}
					for (ExecutionResult exec : execs.getErrored()) {
						Utils.writeToOstream(jobconf.getOutput(), DataSamudayaConstants.NEWLINE);
						Utils.writeToOstream(jobconf.getOutput(), exec.getId().toString());
					}
				}
				numexecute++;
			}
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(), "Number of Executions: " + numexecute);
			}
			if (!completed) {
				return Arrays.asList(new DataCruncherContext<>());
			}
			var dccred = dccmapphases;
			if(!reducer.isEmpty()) {
				dccred = new ArrayList<DataCruncherContext>();
				List<Tuple3Serializable> keyapptasks;
				var dccmapphase = new DataCruncherContext();
				for (var dcc : dccmapphases) {
					dcc.keys().stream().forEach(dcckey -> {
						dcc.get(dcckey).stream().forEach(dccval -> {
							dccmapphase.put(dcckey, dccval);
						});
					});
				}
				keyapptasks = (List<Tuple3Serializable>) dccmapphase.keys().parallelStream()
						.map(key -> new Tuple3Serializable(key, dccmapphase.get(key), getHostPort(dccmapphase.get(key))))
						.collect(Collectors.toCollection(ArrayList::new));
				var partkeys = Iterables
						.partition(keyapptasks, (keyapptasks.size()) / numreducers).iterator();
				log.info("Keys For Shuffling:" + keyapptasks.size());
	
				DexecutorConfig<TaskSchedulerReducerSubmitter, Boolean> redconfig = new DexecutorConfig(newExecutor(), new ReducerTaskExecutor(batchsize, applicationid, dccred));
				DefaultDexecutor<TaskSchedulerReducerSubmitter, Boolean> executorred = new DefaultDexecutor<>(redconfig);
				for (; partkeys.hasNext(); ) {
					mrtaskcount++;
					var currentexecutor = getTaskExecutor(mrtaskcount);
					var rv = new ReducerValues();
					rv.setAppid(applicationid);
					rv.setTuples(new ArrayList<>(partkeys.next()));
					rv.setReducer(reducer.iterator().next());
					var taskid = DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + mrtaskcount;
					var teappid = applicationid;
					if(nonNull(jobconf.isIsuseglobalte()) && jobconf.isIsuseglobalte()) {
						teappid = jobconf.getTeappid();
					}
					var mdtstr = new TaskSchedulerReducerSubmitter(
							currentexecutor, rv, applicationid, taskid, redcount, cf, teappid);
	
					log.debug("Reducer: Submitting " + mrtaskcount + " App And Task:"
							+ applicationid + taskid + rv.getTuples());
					if (!Objects.isNull(jobconf.getOutput())) {
						Utils.writeToOstream(jobconf.getOutput(), "Initial Reducer: Submitting " + mrtaskcount + " App And Task:"
								+ applicationid + taskid + rv.getTuples() + " to " + currentexecutor);
					}
					executorred.addIndependent(mdtstr);
				}
				executorred.execute(ExecutionConfig.NON_TERMINATING);
				log.info("Reducer concluded------------------------------");
				log.info("Total tasks done: " + mrtaskcount);
				if (!isexception) {
					if (!Objects.isNull(jobconf.getOutput())) {
						Utils.writeToOstream(jobconf.getOutput(), "Reducer completed------------------------------");
					}
					var sb = new StringBuilder();
					var partindex = 1;
					for (var ctxreducerpart :dccred) {
						var keysreducers = ctxreducerpart.keys();
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append("Partition " + partindex + "-------------------------------------------------");
						sb.append(DataSamudayaConstants.NEWLINE);
						for (Object key : keysreducers) {
							sb.append(key + DataSamudayaConstants.SINGLESPACE + ctxreducerpart.get(key));
							sb.append(DataSamudayaConstants.NEWLINE);
						}
						sb.append("-------------------------------------------------");
						sb.append(DataSamudayaConstants.NEWLINE);
						sb.append(DataSamudayaConstants.NEWLINE);
						partindex++;
					}
					var filename = DataSamudayaConstants.MAPRED + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
					log.debug("Writing Results to file: " + filename);
					try (var fsdos = hdfs.create(new Path(
							DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + DataSamudayaConstants.FORWARD_SLASH
									+ this.outputfolder + DataSamudayaConstants.FORWARD_SLASH + filename));) {
						fsdos.write(sb.toString().getBytes());
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
				} else {
					if (!Objects.isNull(jobconf.getOutput())) {
						Utils.writeToOstream(jobconf.getOutput(), exceptionmsg);
					}
				}
				jm.setJobcompletiontime(System.currentTimeMillis());
				jm.setTotaltimetaken((jm.getJobcompletiontime() - jm.getJobstarttime()) / 1000.0);
				if (!Objects.isNull(jobconf.getOutput())) {
					Utils.writeToOstream(jobconf.getOutput(),
							"Completed Job in " + ((System.currentTimeMillis() - starttime) / 1000.0) + " seconds");
				}
			}
			return dccred;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error("Unable To Execute Job, See Cause Below:", ex);
		} finally {
			try {
				destroyContainers(applicationid);
				if (!Objects.isNull(cf)) {
					cf.close();
				}
				if (!Objects.isNull(es)) {
					es.shutdown();
				}
			} catch (Exception ex) {
				log.debug("Resource Release Error", ex);
			}

		}
		return null;
	}

	public void reConfigureContainerForStageExecution(TaskSchedulerMapperCombinerSubmitter mdtsstm,
			List<String> availablecontainers) {
		var bsl = (BlocksLocation) mdtsstm.blockslocation;
		var containersgrouped = availablecontainers.stream()
				.collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.UNDERSCORE)[0],
						Collectors.mapping(value -> value, Collectors.toCollection(Vector::new))));
		for (var block : bsl.getBlock()) {
			if (!Objects.isNull(block)) {
				var xrefaddrs = block.getDnxref().keySet().stream().map(dnxrefkey -> {
					return block.getDnxref().get(dnxrefkey);
				}).flatMap(xrefaddr -> xrefaddr.stream()).collect(Collectors.toList());

				var containerdnaddr = (List<Tuple2<String, String>>) xrefaddrs.stream()
						.filter(dnxrefkey -> !Objects.isNull(containersgrouped
								.get(dnxrefkey.split(DataSamudayaConstants.COLON)[0])))
						.map(dnxrefkey -> {
							List<String> containerstosubmitstage = containersgrouped
									.get(dnxrefkey.split(DataSamudayaConstants.COLON)[0]);
							List<Tuple2<String, String>> containerlist = containerstosubmitstage.stream()
									.map(containerhp -> new Tuple2<String, String>(dnxrefkey, containerhp))
									.collect(Collectors.toList());
							return (List<Tuple2<String, String>>) containerlist;
						}).flatMap(containerlist -> containerlist.stream()).collect(Collectors.toList());
				var containerdn = containerdnaddr.get(containercount++ % containerdnaddr.size());
				block.setHp(containerdn.v1);
				bsl.setExecutorhp(containerdn.v2);
				mdtsstm.setHostPort(bsl.getExecutorhp());
			}
		}
	}


	protected class ReducerTaskExecutor implements
			TaskProvider<TaskSchedulerReducerSubmitter, Boolean> {

		Semaphore semaphorereducerresult;
		String applicationid;
		List<DataCruncherContext> dccred;

		protected ReducerTaskExecutor(int batchsize, String applicationid, List<DataCruncherContext> dccred) {
			semaphorereducerresult = new Semaphore(batchsize);
			this.applicationid = applicationid;
			this.dccred = dccred;
		}

		@Override
		public Task<TaskSchedulerReducerSubmitter, Boolean> provideTask(TaskSchedulerReducerSubmitter tsrs) {
			return new Task<TaskSchedulerReducerSubmitter, Boolean>() {
				private static final long serialVersionUID = 8736901461119181694L;

				@Override
				public Boolean execute() {
					try {
						semaphorereducerresult.acquire();
						dccred.add((DataCruncherContext) tsrs.call());
						semaphorereducerresult.release();
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					return tsrs.iscompleted;
				}
			};
		}

	}

	private class MapCombinerTaskExecutor implements
			TaskProvider<TaskSchedulerMapperCombinerSubmitter, Boolean> {

		Semaphore semaphorebatch;
		DataCruncherContext dccmapphase;
		int totaltasks;
		int taskexecuted = 0;
		private Semaphore resultmerge = new Semaphore(1);

		public MapCombinerTaskExecutor(Semaphore semaphore, DataCruncherContext dccmapphase,
				int totaltasks) {
			this.semaphorebatch = semaphore;
			this.dccmapphase = dccmapphase;
			this.totaltasks = totaltasks;
		}

		public com.github.dexecutor.core.task.Task<TaskSchedulerMapperCombinerSubmitter, Boolean> provideTask(
				final TaskSchedulerMapperCombinerSubmitter tsmcs) {

			return new com.github.dexecutor.core.task.Task<TaskSchedulerMapperCombinerSubmitter, Boolean>() {
				private TaskSchedulerMapperCombinerSubmitter tsmcsl = tsmcs;
				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					try {
						semaphorebatch.acquire();						
						RetrieveKeys rk = tsmcsl.call();
						resultmerge.acquire();
						taskexecuted++;
						double percentagecompleted = Math.floor(((float)taskexecuted) / totaltasks * 100.0);
						Utils.writeToOstream( jobconf.getOutput(), "\nPercentage Completed TE("
								+ tsmcsl.getHostPort() + ") " + percentagecompleted + "% \n");
						dccmapphase.putAll(rk.keys, rk.applicationid + rk.taskid);
						resultmerge.release();
						semaphorebatch.release();
						return true;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception ex) {
						log.error("MapCombinerTaskExecutor error", ex);
					}
					return false;
				}
			};
		}

	}

	private ExecutorService newExecutor() {
		return Executors.newWorkStealingPool();
	}

	protected void destroyContainers(String appid) throws Exception {
		var nodes = nodessorted;
		log.debug("Destroying Containers with id:" + appid + " for the hosts: " + nodes);
		var dc = new DestroyContainers();
		for (var node : nodes) {
			dc.setJobid(appid);
			Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
			Resources allocresources = DataSamudayaNodesResources.get().get(node);
			var crs = nodecrsmap.get(node);
			for(ContainerResources cr: crs) {
				long maxmemory = cr.getMaxmemory();
				long directheap = cr.getDirectheap();
				allocresources.setFreememory(allocresources.getFreememory()+maxmemory+directheap);
				allocresources.setNumberofprocessors(allocresources.getNumberofprocessors()+cr.getCpu());
			}
		}
		
	}

	public TaskSchedulerMapperCombinerSubmitter getMassiveDataTaskSchedulerThreadMapperCombiner(
			Set<Object> mapclz,Set<Object> combiners, BlocksLocation blockslocation, ApplicationTask apptask, String applicationid) throws Exception {
		log.debug("Block To Read :" + blockslocation);
		if(nonNull(jobconf.isIsuseglobalte()) && jobconf.isIsuseglobalte()) {
			applicationid = jobconf.getTeappid();
		}
		var mdtstmc = new TaskSchedulerMapperCombinerSubmitter(
				blockslocation, true, new LinkedHashSet<>(mapclz), combiners,
				cf, apptask, applicationid);
		apptask.setHp(blockslocation.getExecutorhp());
		blocklocationindex++;
		return mdtstmc;
	}

	public String getTaskExecutor(long blocklocationindex) {
		return containers.get((int) blocklocationindex % containers.size());
	}
}
