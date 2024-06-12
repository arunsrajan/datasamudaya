/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.stream.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.IntSupplier;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.IgniteCache;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyOutputStream;

import com.github.datasamudaya.common.AllocateContainers;
import com.github.datasamudaya.common.Block;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ContainerLaunchAttributes;
import com.github.datasamudaya.common.ContainerResources;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaIgniteClient;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.DataSamudayaUsers;
import com.github.datasamudaya.common.DestroyContainer;
import com.github.datasamudaya.common.DestroyContainers;
import com.github.datasamudaya.common.EXECUTORTYPE;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.GlobalContainerLaunchers;
import com.github.datasamudaya.common.GlobalJobFolderBlockLocations;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.AbstractPipeline;
import com.github.datasamudaya.stream.CsvOptionsSQL;
import com.github.datasamudaya.stream.JsonSQL;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.StreamPipeline;

public class FileBlocksPartitionerHDFS {
	private static final Logger log = Logger.getLogger(FileBlocksPartitionerHDFS.class);
	protected long totallength;
	protected FileSystem hdfs;
	protected List<Path> filepaths = new ArrayList<>();
	protected List<String> containers;
	protected Set<String> nodeschoosen;
	protected IntSupplier supplier;
	protected Job job;
	protected PipelineConfig pipelineconfig;
	protected List<String> nodessorted;
	ConcurrentMap<String, Resources> resources;
	CountDownLatch cdl;
	List<String> containerswithhostport;
	ZookeeperOperations zookeeperoperations = new ZookeeperOperations();

	Boolean ismesos, isyarn, islocal, isjgroups, isblocksuserdefined, isignite;
	List<Integer> ports;

	public List<ContainerResources> getTotalMemoryContainersReuseAllocation(String nodehp, int containerstoallocate) {
		var containers = GlobalContainerAllocDealloc.getNodecontainers().get(nodehp);
		var cres = new ArrayList<ContainerResources>();
		if (!Objects.isNull(containers) && !containers.isEmpty()) {
			int contcount = 0;
			for (String container : containers) {
				cres.add(GlobalContainerAllocDealloc.getHportcrs().get(container));
				contcount++;
				if (contcount >= containerstoallocate) {
					break;
				}
			}
		}
		return cres;
	}

	/**
	 * The block size is determined by sum of length of all files divided by number
	 * of partition.
	 * 
	 * @return
	 * @throws Exception
	 */
	protected List<BlocksLocation> getHDFSParitions() throws PipelineException {
		var numpartition = this.supplier.getAsInt();
		totallength = 0;
		try {
			totallength = Utils.getTotalLengthByFiles(hdfs, filepaths);
			var blocksize = totallength / numpartition;

			return getBlocks(null);
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEIOERROR, ex);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ex);
		}
	}

	private final ConcurrentMap<Stage, Object> stageoutputmap = new ConcurrentHashMap<>();
	private final ConcurrentMap<String, String> allstageshostport = new ConcurrentHashMap<>();

	/**
	 * Get File Blocks for job
	 * 
	 * @param job
	 * @throws PipelineException
	 * @throws URISyntaxException
	 * @throws IOException
	 * @throws Exception
	 */
	@SuppressWarnings({"rawtypes"})
	public void getJobStageBlocks(Job job, IntSupplier supplier, String protocol, Set<Stage> rootstages,
			Collection<AbstractPipeline> mdsroots, int blocksize, PipelineConfig pc)
			throws PipelineException, IOException, URISyntaxException {
		try {
			log.debug("Partitioning of Blocks started...");
			this.job = job;
			this.pipelineconfig = pc;
			var roots = mdsroots.iterator();
			var noofpartition = 0l;
			ismesos = Boolean.parseBoolean(pc.getMesos());
			isyarn = Boolean.parseBoolean(pc.getYarn());
			islocal = Boolean.parseBoolean(pc.getLocal());
			isjgroups = Boolean.parseBoolean(pc.getJgroups());
			isblocksuserdefined = Boolean.parseBoolean(pc.getIsblocksusedefined()) || supplier != null;
			isignite = Objects.isNull(pc.getMode()) ? false
					: pc.getMode().equals(DataSamudayaConstants.MODE_DEFAULT) ? true : false;
			if (Boolean.TRUE.equals(islocal) || Boolean.TRUE.equals(ismesos) || Boolean.TRUE.equals(isyarn)
					|| Boolean.TRUE.equals(isignite)) {
				nodeschoosen = new HashSet<>(Arrays.asList(DataSamudayaConstants.DUMMYNODE));
				containers = Arrays.asList(DataSamudayaConstants.DUMMYCONTAINER);
			}

			var totalblockslocation = new ArrayList<BlocksLocation>();
			String hdfspath = null, folder = null;
			List<String> columns = null;
			List<Path> metricsfilepath = new ArrayList<>();
			job.getJm().setTotalfilesize(0);
			List<String> folderstolbcontainers = new ArrayList<>();
			for (var rootstage : rootstages) {
				var obj = roots.next();
				if (!CollectionUtils.isEmpty(rootstage.tasks) && rootstage.tasks.get(0) instanceof CsvOptionsSQL csvOptionsSQL) {
					columns = csvOptionsSQL.getRequiredcolumns();
				} else if (!CollectionUtils.isEmpty(rootstage.tasks) && rootstage.tasks.get(0) instanceof JsonSQL jsonSQL) {
					columns = jsonSQL.getRequiredcolumns();
				}
				if (obj instanceof StreamPipeline mdp) {
					hdfspath = mdp.getHdfspath();
					folder = mdp.getFolder();
				}
				if (isNull(folder)) {
					continue;
				}
				if (isNull(hdfspath)) {
					hdfspath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
				}
				try (var hdfs = FileSystem.newInstance(new URI(hdfspath), new Configuration());) {
					this.hdfs = hdfs;
					this.filepaths.clear();
					List<Path> newpaths = getFilePaths(hdfspath, folder);
					this.filepaths.addAll(newpaths);
					job.getJm().setTotalfilesize(
							job.getJm().getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, filepaths));
					if (pc.getUseglobaltaskexecutors()
							&& nonNull(GlobalJobFolderBlockLocations.get(pc.getTejobid(), folder))) {
						Set<Path> pathtoprocess = GlobalJobFolderBlockLocations.compareCurrentPathsNewPathsAndStore(pc.getTejobid(), folder, newpaths, hdfs);
						List<BlocksLocation> bls = GlobalJobFolderBlockLocations.get(pc.getTejobid(), folder);
						if (CollectionUtils.isNotEmpty(pathtoprocess)) {
							this.filepaths.clear();
							this.filepaths.addAll(pathtoprocess);
							List<BlocksLocation> blsnew = getBlocks(columns);
							if (isjgroups || !islocal && !isyarn && !ismesos && !isignite) {
								getDnXref(blsnew, true);
								getContainersGlobal();
								allocateContainersLoadBalanced(blsnew);
							}
							List<String> files = newpaths.parallelStream().map(path -> path.toUri().toString()).toList();
							List<String> filestoprocess = pathtoprocess.parallelStream().map(path -> path.toUri().toString()).toList();
							List<BlocksLocation> currentbls = new ArrayList<>(bls);
							for (BlocksLocation bltoremove :currentbls) {
								Block[] block = bltoremove.getBlock();
								if (!files.contains(block[0].getFilename())
										|| filestoprocess.contains(block[0].getFilename())) {
									bls.remove(bltoremove);
								}
							}
							blsnew.stream().forEach(blocks -> blocks.setToreprocess(true));
							bls.addAll(blsnew);
						} else {
							List<String> files = newpaths.parallelStream().map(path -> path.toUri().toString()).toList();
							List<BlocksLocation> currentbls = new ArrayList<>(bls);
							for (BlocksLocation bltoremove :currentbls) {
								Block[] block = bltoremove.getBlock();
								bltoremove.setToreprocess(false);
								if (!files.contains(block[0].getFilename())) {
									bls.remove(bltoremove);
								}
							}
						}
						final List<String> columnsql = columns;
						bls.parallelStream().forEach(bl -> {
							bl.setColumns(columnsql);
						});
						totalblockslocation.addAll(bls);
						stageoutputmap.put(rootstage, bls);
						noofpartition += bls.size();
						GlobalJobFolderBlockLocations.putPaths(hdfspath, folder, newpaths, hdfs);
					} else {
						folderstolbcontainers.add(folder);
						if (!stageoutputmap.containsKey(rootstage)) {
							List blocks = null;
							if (supplier instanceof IntSupplier) {
								this.supplier = supplier;
								blocks = getHDFSParitions();
								if (pc.getUseglobaltaskexecutors()) {
									GlobalJobFolderBlockLocations.put(pc.getTejobid(), folder, blocks);
									GlobalJobFolderBlockLocations.putPaths(pc.getTejobid(), folder, newpaths, hdfs);
								}
								totalblockslocation.addAll(blocks);
							} else {
								// Get block if HDFS protocol.
								blocks = getBlocks(columns);
								if (pc.getUseglobaltaskexecutors()) {
									GlobalJobFolderBlockLocations.put(pc.getTejobid(), folder, blocks);
									GlobalJobFolderBlockLocations.putPaths(pc.getTejobid(), folder, newpaths, hdfs);
								}
								totalblockslocation.addAll(blocks);
							}
							if (blocks == null) {
								throw new PipelineException(DataSamudayaConstants.DATABLOCK_EXCEPTION);
							}
							stageoutputmap.put(rootstage, blocks);
							noofpartition += blocks.size();
						}
					}
				}
				metricsfilepath.addAll(filepaths);
			}
			job.getJm().setFiles(Utils.getAllFilePaths(metricsfilepath));
			job.getJm().setTotalfilesize(job.getJm().getTotalfilesize() / DataSamudayaConstants.MB);
			job.getJm().setTotalblocks(totalblockslocation.size());
			if (isignite) {
				getDnXref(totalblockslocation, false);
				if (pipelineconfig.getStorage() != STORAGE.COLUMNARSQL) {
					sendDataToIgniteServer(totalblockslocation,
							((StreamPipeline) mdsroots.iterator().next()).getHdfspath());
				} else {
					initializeIgniteServer();
				}
			} else if (isjgroups || !islocal && !isyarn && !ismesos) {
				getDnXref(totalblockslocation, true);
				if (!pc.getUseglobaltaskexecutors()) {
					allocateContainersByResources(totalblockslocation);
					allocateContainersLoadBalanced(totalblockslocation);
				} else {
					getContainersGlobal();
					for (String foldertolb :folderstolbcontainers) {
						allocateContainersLoadBalanced(GlobalJobFolderBlockLocations.get(pc.getTejobid(), foldertolb));
					}
				}
				job.getJm().setNodes(nodeschoosen);
				job.getJm().setContainersallocated(new ConcurrentHashMap<>());
			} else if (islocal || ismesos) {
				getDnXref(totalblockslocation, false);
			} else if(isyarn) {
				getDnXref(totalblockslocation, false);
				if (pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
					try(ZookeeperOperations zo=new ZookeeperOperations();){
						zo.connect();
						containers = zo.getTaskExectorsByJobId(pipelineconfig.getTejobid());
						allocateContainersLoadBalanced(totalblockslocation);
					}
				}
			}
			job.setNoofpartitions(noofpartition);
			if (job.getStageoutputmap() != null) {
				job.getStageoutputmap().putAll(stageoutputmap);
			} else {
				job.setStageoutputmap(stageoutputmap);
			}
			job.setAllstageshostport(allstageshostport);
			log.debug("Partitioning of Blocks ended.");
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEBLOCKSPARTITIONINGERROR, ex);
			destroyTaskExecutors();			
			throw new PipelineException(PipelineConstants.FILEBLOCKSPARTITIONINGERROR, ex);
		}
	}


	/**
	 * The blocks data is fetched from hdfs and caches in Ignite Server
	 * 
	 * @param totalblockslocation
	 * @param hdfspath
	 * @throws Exception
	 */
	protected void sendDataToIgniteServer(List<BlocksLocation> totalblockslocation, String hdfspath) throws Exception {
		// Starting the node
		var ignite = DataSamudayaIgniteClient.instance(pipelineconfig);
		IgniteCache<Object, byte[]> ignitecache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
		try (var hdfs = FileSystem.newInstance(new URI(hdfspath), new Configuration());) {
			for (var bsl : totalblockslocation) {
				job.getInput().add(bsl);// fetch the block data from hdfs
				var databytes = HdfsBlockReader.getBlockDataMR(bsl, hdfs);
				var baos = new ByteArrayOutputStream();
				var lzfos = new SnappyOutputStream(baos);
				lzfos.write(databytes);
				lzfos.flush();
				// put hdfs block data to ignite sesrver
				ignitecache.put(bsl, baos.toByteArray());
				lzfos.close();
			}
		}
		job.setIgcache(ignitecache);
		job.setIgnite(ignite);
		var computeservers = job.getIgnite().cluster().forServers();
		job.getJm().setContainersallocated(
				computeservers.hostNames().stream().collect(Collectors.toMap(key -> key, value -> 0d)));
	}

	/**
	 * The blocks data is fetched from hdfs and caches in Ignite Server
	 * 
	 * @param totalblockslocation
	 * @param hdfspath
	 * @throws Exception
	 */
	protected void initializeIgniteServer() throws Exception {
		// Starting the node
		var ignite = DataSamudayaIgniteClient.instance(pipelineconfig);
		IgniteCache<Object, byte[]> ignitecache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
		job.setIgcache(ignitecache);
		job.setIgnite(ignite);
		var computeservers = job.getIgnite().cluster().forServers();
		job.getJm().setContainersallocated(
				computeservers.hostNames().stream().collect(Collectors.toMap(key -> key, value -> 0d)));
	}

	/**
	 * Destroy the allocated containers.
	 * 
	 * @throws PipelineException
	 */
	protected void destroyTaskExecutors() throws PipelineException {
		try {
			// Global semaphore to allocated and deallocate containers.
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			if (!Objects.isNull(job.getNodes())) {
				var nodes = job.getNodes();
				var jobcontainerids = GlobalContainerAllocDealloc.getJobcontainerids();
				var chpcres = GlobalContainerAllocDealloc.getHportcrs();
				var deallocateall = true;
				if (!Objects.isNull(job.getTaskexecutors())) {
					var cids = jobcontainerids.get(job.getId());
					// Obtain containers from job
					for (String te : job.getTaskexecutors()) {
						if (!cids.isEmpty()) {
							cids.remove(te);
							jobcontainerids.remove(te);
							var dc = new DestroyContainer();
							dc.setJobid(job.getId());
							dc.setContainerhp(te);
							// Remove the container from global container node map
							String node = GlobalContainerAllocDealloc.getContainernode().remove(te);
							Set<String> containers = GlobalContainerAllocDealloc.getNodecontainers().get(node);
							containers.remove(te);
							// Remove the container from the node and destroy it.
							Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
							ContainerResources cr = chpcres.remove(te);
							Resources allocresources = DataSamudayaNodesResources.get().get(node);
							if (!pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE)) {
								long maxmemory = cr.getMaxmemory();
								long directheap = cr.getDirectheap();
								allocresources.setFreememory(allocresources.getFreememory() + maxmemory + directheap);
								allocresources
										.setNumberofprocessors(allocresources.getNumberofprocessors() + cr.getCpu());
							} else {
								var usersshare = DataSamudayaUsers.get();
								var user = usersshare.get(pipelineconfig.getUser());
							}
						} else {
							deallocateall = false;
						}
					}
				}
				if (deallocateall) {
					var dc = new DestroyContainers();
					dc.setJobid(job.getId());
					log.debug("Destroying Containers with id:" + job.getId() + " for the hosts: " + nodes);
					// Destroy all the containers from all the nodes
					for (var node : nodes) {
						Utils.getResultObjectByInput(node, dc, DataSamudayaConstants.EMPTY);
					}
				}
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.DESTROYCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.DESTROYCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Get Paths of files in HDFS given the relative folder path
	 * 
	 * @param hdfspth
	 * @param folder
	 * @return List of file paths
	 * @throws PipelineException
	 */
	protected List<Path> getFilePaths(String hdfspth, String folder) throws PipelineException {
		try {
			var fileStatuses = new ArrayList<FileStatus>();
			var fileStatus = hdfs.listFiles(new Path(hdfspth + folder), true);
			while (fileStatus.hasNext()) {
				fileStatuses.add(fileStatus.next());
			}
			var paths = FileUtil.stat2Paths(fileStatuses.toArray(new FileStatus[fileStatuses.size()]));
			return Arrays.asList(paths);
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEPATHERROR, ex);
			throw new PipelineException(PipelineConstants.FILEPATHERROR, ex);
		}
	}

	/**
	 * Get locations of blocks user defined.
	 * 
	 * @param isblocksuserdefined
	 * @param blocksize
	 * @return
	 * @throws PipelineException
	 */
	protected List<BlocksLocation> getBlocks(List<String> columns) throws PipelineException {
		try {
			List<BlocksLocation> bls = null;
			// Fetch the location of blocks for user defined block size.
			bls = HDFSBlockUtils.getBlocksLocation(hdfs, filepaths, columns);
			return bls;
		} catch (Exception ex) {
			log.error(PipelineConstants.FILEBLOCKSERROR, ex);
			throw new PipelineException(PipelineConstants.FILEBLOCKSERROR, ex);
		}
	}

	/**
	 * Containers with balanced allocation.
	 * 
	 * @param bls
	 * @throws PipelineException
	 */
	protected void allocateContainersLoadBalanced(List<BlocksLocation> bls) throws PipelineException {
		log.debug("Entered FileBlocksPartitionerHDFS.getContainersBalanced");
		var hostcontainermap = containers.stream()
				.collect(Collectors.groupingBy(key -> pipelineconfig.getIspodcidrtonodemappingenabled()?
						Utils.getNodeIPByPodIP(key.split(DataSamudayaConstants.UNDERSCORE)[0]).get()
						:key.split(DataSamudayaConstants.UNDERSCORE)[0],
						Collectors.mapping(container -> container, Collectors.toCollection(ArrayList::new))));
		var containerallocatecount = (Map<String, Long>) containers.stream()
				.collect(Collectors.toMap(container -> container, container -> 0l, (val1, val2)->val1+val2));
		List<String> hostportcontainer;
		// Iterate over the blocks location
		for (var b : bls) {
			hostportcontainer = hostcontainermap.get(b.getBlock()[0].getHp().split(DataSamudayaConstants.COLON)[0]);
			if (Objects.isNull(hostportcontainer)) {
				throw new PipelineException(PipelineConstants.INSUFFNODESFORDATANODEERROR
						.replace("%s", b.getBlock()[0].getHp()).replace("%d", hostcontainermap.toString()));
			}
			// Find the container from minimum to maximum in ascending sorted order.
			var optional = hostportcontainer.stream().sorted((xref1, xref2) -> {
				return containerallocatecount.get(xref1).compareTo(containerallocatecount.get(xref2));
			}).findFirst();
			if (optional.isPresent()) {
				var container = optional.get();
				// Assign the minimal allocated container host port to blocks location.
				b.setExecutorhp(container);
				containerallocatecount.put(container, containerallocatecount.get(container) + 1);
			} else {
				throw new PipelineException(PipelineConstants.CONTAINERALLOCATIONERROR);
			}
		}
		log.debug("Exiting FileBlocksPartitionerHDFS.getContainersBalanced");
	}

	/**
	 * Allocate the datanode host port to blocks host port considering load
	 * balanced.
	 * 
	 * @param bls
	 * @param issa
	 * @throws PipelineException
	 */
	public void getDnXref(List<BlocksLocation> bls, boolean issa) throws PipelineException {
		log.debug("Entered FileBlocksPartitionerHDFS.getDnXref");
		// Get all the datanode's host port for the job hdfs folder
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (;xref.hasNext();) {
				xrefs.addAll(xref.next());
			}
			if (bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
				for (;xref.hasNext();) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		// Initialize the map with key as datanodes host with port as key and allocation
		// count as 0
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));
		// Perform the datanode allocation to blocks for the standalone scheduler
		if (issa) {
			resources = DataSamudayaNodesResources.get();
			// Obtain all the nodes.
			var computingnodes = resources.keySet().stream().map(node -> pipelineconfig.getIspodcidrtonodemappingenabled()?Utils.getNodeIPByPodIP(node.split(DataSamudayaConstants.UNDERSCORE)[0]).get():
					node.split(DataSamudayaConstants.UNDERSCORE)[0])
					.collect(Collectors.toList());
			// Iterate the blocks location and assigned the balanced allocated datanode
			// hostport to blocks
			// object.
			for (var b : bls) {
				// Get first minimal allocated datanode hostport;
				var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
						.filter(xrefhost -> computingnodes.contains(xrefhost))
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream())
						.sorted((xref1, xref2) -> {
							return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
						}).findFirst();
				if (xrefselected.isEmpty()) {
					throw new PipelineException(PipelineConstants.INSUFFNODESERROR + " Available computing nodes are "
							+ computingnodes + " Available Data Nodes are " + b.getBlock()[0].getDnxref().keySet());
				}
				// Get the datanode selected
				final var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				// Assign the datanode hp to blocks.
				b.getBlock()[0].setHp(xref);
				// Perform the same steps for second block.
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
							throw new PipelineException(PipelineConstants.INSUFFNODESERROR
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
						.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream())
						.sorted((xref1, xref2) -> {
							return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
						}).findFirst();
				var xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[0].setHp(xref);
				if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
					xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
							.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream())
							.sorted((xref1, xref2) -> {
								return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
							}).findFirst();
					xref = xrefselected.get();
					b.getBlock()[1].setHp(xref);
				}
			}
		}
		log.debug("Exiting FileBlocksPartitionerHDFS.getDnXref");
	}

	/**
	 * Reuse allocated containers for new job.
	 * 
	 * @param nodehp
	 * @param totalmemorytoalloc
	 * @param totalallocated
	 * @return
	 */
	public List<ContainerResources> getTotalMemoryContainersReuseAllocation(String nodehp, long totalmemorytoalloc,
			AtomicLong totalallocated) {
		var containers = GlobalContainerAllocDealloc.getNodecontainers().get(nodehp);
		var cres = new ArrayList<ContainerResources>();
		if (!Objects.isNull(containers)) {
			long memtotal = 0;
			// Iterate containers and allocate all the containers till the total memory
			// allocated
			// is less than the memory to allocate.
			for (String container : containers) {
				ContainerResources crs = GlobalContainerAllocDealloc.getHportcrs().get(container);
				if (!Objects.isNull(crs)) {
					cres.add(crs);
					memtotal += crs.getMaxmemory();
				}
				if (memtotal >= totalmemorytoalloc) {
					break;
				}
			}
			totalallocated.set(memtotal);
		}
		return cres;
	}

	/**
	 * Allocate Containers by Resources (Processor, Memory)
	 * 
	 * @param bls
	 * @throws PipelineException
	 */
	protected void allocateContainersByResources(List<BlocksLocation> bls) throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			containers = new ArrayList<>();
			nodeschoosen = new HashSet<>();
			var loadjar = new LoadJar();
			loadjar.setMrjar(pipelineconfig.getJar());
			var totalcontainersallocated = 0;
			var nodestotalblockmem = new ConcurrentHashMap<String, Long>();
			// Get all the nodes in sort by processor and then by memory.
			getNodesResourcesSorted(bls, nodestotalblockmem);
			job.setLcs(new ArrayList<>());
			// Iterate over the sorted nodes.
			for (var node : nodessorted) {
				var host = node.split("_")[0];
				var lc = new LaunchContainers();
				lc.setNodehostport(node);
				lc.setJobid(job.getId());
				lc.setMode(isignite ? LaunchContainers.MODE.IGNITE : LaunchContainers.MODE.NORMAL);
				var cla = new ContainerLaunchAttributes();
				AtomicLong totalallocated = new AtomicLong();
				// Get Reused containers to be allocated by current job.
				// Calculate the remaining to allocate.
				List<ContainerResources> contres = null;

				if (Objects.isNull(resources.get(node))) {
					throw new PipelineException(PipelineConstants.RESOURCESDOWNRESUBMIT.replace("%s", node));
				}
				// Allocate the remaining memory from total allocated on the host.
				contres = getContainersByNodeResourcesRemainingMemory(pipelineconfig.getGctype(),
						nodestotalblockmem.get(host), resources.get(node));

				job.getLcs().add(lc);
				ports = null;
				if (!Objects.isNull(contres) && !contres.isEmpty()) {
					cla.setNumberofcontainers(contres.size());
					cla.setCr(contres);
					lc.setCla(cla);
					var ac = new AllocateContainers();
					ac.setJobid(job.getId());
					ac.setNumberofcontainers(contres.size());
					// Allocate the containers via node and return the allocated port.
					ports = (List<Integer>) Utils.getResultObjectByInput(node, ac, DataSamudayaConstants.EMPTY);
				}
				if (Objects.isNull(ports) || ports.isEmpty()) {
					continue;
				}
				Resources allocresources = resources.get(node);
				// Iterate containers to add the containers to global allocation.
				for (int containercount = 0;containercount < ports.size();containercount++) {
					ContainerResources crs = contres.get(containercount);
					if (!pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE)) {
						long maxmemory = crs.getMaxmemory();
						long directheap = crs.getDirectheap();
						allocresources.setFreememory(allocresources.getFreememory() - maxmemory - directheap);
						allocresources.setNumberofprocessors(allocresources.getNumberofprocessors() - crs.getCpu());
					}
					crs.setPort(ports.get(containercount));
					String conthp = host + DataSamudayaConstants.UNDERSCORE + ports.get(containercount);
					containers.add(conthp);
					var containerids = GlobalContainerAllocDealloc.getJobcontainerids().get(job.getId());
					if (Objects.isNull(containerids)) {
						containerids = new ArrayList<>();
						GlobalContainerAllocDealloc.getJobcontainerids().put(job.getId(), containerids);
					}
					containerids.add(conthp);
					GlobalContainerAllocDealloc.getHportcrs().put(conthp, crs);
					GlobalContainerAllocDealloc.getContainernode().put(conthp, node);
				}
				Set<String> contallocated = GlobalContainerAllocDealloc.getNodecontainers().get(node);
				if (Objects.isNull(contallocated)) {
					contallocated = new LinkedHashSet<>();
					GlobalContainerAllocDealloc.getNodecontainers().put(node, contallocated);
				}
				contallocated.addAll(containers);
				totalcontainersallocated += contres.size();
				nodeschoosen.add(node);
			}
			job.setTaskexecutors(containers);
			job.setNodes(nodeschoosen);
			// Get the node and container assign to job metrics for display.
			setContainerResources();
			log.debug("Total Containers Allocated:" + totalcontainersallocated);
		} catch (Exception ex) {
			log.error(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
			throw new PipelineException(PipelineConstants.TASKEXECUTORSALLOCATIONERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}


	private void setContainerResources() {
		job.getJm().setContainerresources(job.getLcs().stream().flatMap(lc -> {
			var crs = lc.getCla().getCr();
			return crs.stream().map(cr -> {
				var node = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE)[0];
				var cpu = cr.getCpu();
				var maxmemory = cr.getMaxmemory();
				var directmemory = cr.getDirectheap();
				var port = cr.getPort();
				return DataSamudayaConstants.BR + node + DataSamudayaConstants.UNDERSCORE + port + DataSamudayaConstants.COLON
						+ DataSamudayaConstants.BR + DataSamudayaConstants.CPUS + DataSamudayaConstants.EQUAL + cpu + DataSamudayaConstants.BR
						+ DataSamudayaConstants.MEM + DataSamudayaConstants.EQUAL + maxmemory + DataSamudayaConstants.ROUNDED_BRACKET_OPEN
						+ (Math.floor(maxmemory / (double) (maxmemory + directmemory) * 100.0))
						+ DataSamudayaConstants.ROUNDED_BRACKET_CLOSE + DataSamudayaConstants.BR + DataSamudayaConstants.DIRECTMEM
						+ DataSamudayaConstants.EQUAL + directmemory + DataSamudayaConstants.ROUNDED_BRACKET_OPEN
						+ (Math.floor(directmemory / (double) (maxmemory + directmemory) * 100.0))
						+ DataSamudayaConstants.ROUNDED_BRACKET_CLOSE;

			}).collect(Collectors.toList()).stream();
		}).collect(Collectors.toList()));
	}

	/**
	 * Get container and nodes from LaunchContainers list object.
	 */
	protected void getContainersGlobal() {
		job.setLcs(GlobalContainerLaunchers.get(pipelineconfig.getUser(), pipelineconfig.getTejobid()));
		job.setId(pipelineconfig.getJobid());
		// Get containers
		LaunchContainers launchcontainer = job.getLcs().get(0);
		ContainerResources continerresources = launchcontainer.getCla().getCr().get(0);
		if(continerresources.getExecutortype() == EXECUTORTYPE.DRIVER) {
			if(launchcontainer.getCla().getCr().size()>1) {
				launchcontainer.getCla().getCr().remove(0);
			} else {
				job.getLcs().remove(0);
			}
		}
		containers = job.getLcs().stream().flatMap(lc -> {
			var host = lc.getNodehostport().split(DataSamudayaConstants.UNDERSCORE);
			return lc.getCla().getCr().stream().map(cr -> {
				return host[0] + DataSamudayaConstants.UNDERSCORE + cr.getPort();
			}).collect(Collectors.toList()).stream();
		}).collect(Collectors.toList());
		job.setTaskexecutors(containers);
		// Get nodes
		job.setNodes(job.getLcs().stream().map(lc -> lc.getNodehostport()).collect(Collectors.toSet()));
		setContainerResources();
	}

	/**
	 * Get nodes resources sorted in ascending of processors and then memory
	 * 
	 * @param bls
	 * @param nodestotalblockmem
	 */
	protected void getNodesResourcesSorted(List<BlocksLocation> bls, Map<String, Long> nodestotalblockmem) {
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
					nodestotalblockmem.put(block1.getHp().split(DataSamudayaConstants.COLON)[0],
							value + (block1.getBlockend() - block1.getBlockstart()));
				} else {
					nodestotalblockmem.put(block1.getHp().split(DataSamudayaConstants.COLON)[0],
							block1.getBlockend() - block1.getBlockstart());
				}
			}
			if (!Objects.isNull(block2)) {
				xref.add(block2.getHp().split(DataSamudayaConstants.COLON)[0]);
				var value = nodestotalblockmem.get(block2.getHp().split(DataSamudayaConstants.COLON)[0]);
				if (value != null) {
					nodestotalblockmem.put(block2.getHp().split(DataSamudayaConstants.COLON)[0],
							value + (block2.getBlockend() - block2.getBlockstart()));
				} else {
					nodestotalblockmem.put(block2.getHp().split(DataSamudayaConstants.COLON)[0],
							block2.getBlockend() - block2.getBlockstart());
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
				} else {
					return 1;
				}
			} else {
				return 1;
			}
		}).map(entry -> entry.getKey()).filter(key -> nodeswithhostonly.contains(key.split(DataSamudayaConstants.UNDERSCORE)[0]))
				.collect(Collectors.toList());
	}

	/**
	 * Get container resources allocated based on total memory with options combined
	 * and divided.
	 * 
	 * @param gctype
	 * @param totalmem
	 * @param resources
	 * @return
	 * @throws PipelineException
	 */
	protected List<ContainerResources> getContainersByNodeResourcesRemainingMemory(String gctype, long totalmem,
			Resources resources) throws PipelineException {
		var cpu = resources.getNumberofprocessors() - 1;
		var cr = new ArrayList<ContainerResources>();
		if (pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT)) {
			var res = new ContainerResources();
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			res.setCpu(cpu);
			var memoryrequire = actualmemory;
			var heapmem = memoryrequire * Integer.valueOf(pipelineconfig.getHeappercent()) / 100;
			res.setMinmemory(heapmem);
			res.setMaxmemory(heapmem);
			res.setDirectheap(memoryrequire - heapmem);
			res.setGctype(gctype);
			res.setExecutortype(EXECUTORTYPE.EXECUTOR);
			cr.add(res);
			return cr;
		} else if (pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_DIVIDED)) {
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var numofcontainerspermachine = Integer.parseInt(pipelineconfig.getNumberofcontainers());
			var dividedcpus = cpu / numofcontainerspermachine;
			var maxmemory = actualmemory / numofcontainerspermachine;
			var numberofcontainer = 0;
			while (true) {
				if (cpu >= dividedcpus && actualmemory >= 0) {
					var res = new ContainerResources();
					res.setCpu(dividedcpus);
					var heapmem = maxmemory * Integer.valueOf(pipelineconfig.getHeappercent()) / 100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(maxmemory - heapmem);
					res.setGctype(gctype);
					res.setExecutortype(EXECUTORTYPE.EXECUTOR);
					cpu -= dividedcpus;
					actualmemory -= maxmemory;
					cr.add(res);
				} else if (cpu >= 1 && actualmemory >= 0) {
					var res = new ContainerResources();
					res.setCpu(cpu);
					var heapmem = maxmemory * Integer.valueOf(pipelineconfig.getHeappercent()) / 100;
					res.setMinmemory(heapmem);
					res.setMaxmemory(heapmem);
					res.setDirectheap(maxmemory - heapmem);
					res.setGctype(gctype);
					res.setExecutortype(EXECUTORTYPE.EXECUTOR);
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
		} else if (pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_IMPLICIT)) {
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			var numberofimplicitcontainers = Integer.valueOf(pipelineconfig.getImplicitcontainerallocanumber());
			var numberofimplicitcontainercpu = Integer.valueOf(pipelineconfig.getImplicitcontainercpu());
			var numberofimplicitcontainermemory = pipelineconfig.getImplicitcontainermemory();
			var numberofimplicitcontainermemorysize = Long.valueOf(pipelineconfig.getImplicitcontainermemorysize());
			var memorysize = "GB".equals(numberofimplicitcontainermemory) ? DataSamudayaConstants.GB : DataSamudayaConstants.MB;
			var memoryrequired = numberofimplicitcontainermemorysize * memorysize;
			if (actualmemory < memoryrequired * numberofimplicitcontainers) {
				throw new PipelineException(PipelineConstants.INSUFFMEMORYALLOCATIONERROR);
			}
			if (cpu < numberofimplicitcontainercpu * numberofimplicitcontainers) {
				throw new PipelineException(PipelineConstants.INSUFFCPUALLOCATIONERROR);
			}
			for (var count = 0;count < numberofimplicitcontainers;count++) {
				var res = new ContainerResources();
				res.setCpu(numberofimplicitcontainercpu);
				var heapmem = memoryrequired * Integer.valueOf(pipelineconfig.getHeappercent()) / 100;
				res.setMinmemory(heapmem);
				res.setMaxmemory(heapmem);
				res.setDirectheap(memoryrequired - heapmem);
				res.setGctype(gctype);
				res.setExecutortype(EXECUTORTYPE.EXECUTOR);
				cr.add(res);
			}
			return cr;
		} else if (pipelineconfig.getContaineralloc().equals(DataSamudayaConstants.CONTAINER_ALLOC_USERSHARE)) {
			var res = new ContainerResources();
			var actualmemory = resources.getFreememory() - DataSamudayaConstants.GB;
			if (actualmemory < (128 * DataSamudayaConstants.MB)) {
				throw new PipelineException(PipelineConstants.MEMORYALLOCATIONERROR);
			}
			var usersshare = DataSamudayaUsers.get();
			var user = usersshare.get(pipelineconfig.getUser());
			if (isNull(user)) {
				throw new PipelineException(
						PipelineConstants.USERNOTCONFIGURED.formatted(pipelineconfig.getUser()));
			}
			cpu = cpu * user.getPercentage() / 100;
			res.setCpu(cpu);
			var memoryrequire = actualmemory * user.getPercentage() / 100;
			var heapmem = memoryrequire * Integer.valueOf(pipelineconfig.getHeappercent()) / 100;
			res.setMinmemory(heapmem);
			res.setMaxmemory(heapmem);
			res.setDirectheap(memoryrequire - heapmem);
			res.setGctype(gctype);
			res.setExecutortype(EXECUTORTYPE.EXECUTOR);
			cr.add(res);
			return cr;
		} else {
			throw new PipelineException(PipelineConstants.UNSUPPORTEDMEMORYALLOCATIONMODE);
		}
	}
}
