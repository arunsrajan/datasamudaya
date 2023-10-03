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
package com.github.datasamudaya.stream.utils;

import static org.junit.Assert.assertEquals;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.StreamPipelineBaseTestCommon;
import com.github.datasamudaya.stream.utils.FileBlocksPartitionerHDFS;
import com.github.datasamudaya.tasks.executor.NodeRunner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class FileBlocksPartitionerHDFSMultipleNodesTest extends StreamPipelineBaseTestCommon {
	private static final int NOOFNODES = 5;
	static int teport = 12121;
	static ExecutorService escontainer;
	static ConcurrentMap<String, List<ServerSocket>> containers;
	static ConcurrentMap<String, List<Thread>> tes;
	static List<Registry> containerlauncher = new ArrayList<>();
	static Logger log = Logger.getLogger(FileBlocksPartitionerHDFSMultipleNodesTest.class);
	static int nodeindex;
	static FileSystem hdfs;
	static Path[] paths;
	static List<BlocksLocation> bls;
	static Registry server = null;
	private static ZookeeperOperations zo;
	
	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
		hdfs = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());
		FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsfilepath + airlinesample));
		paths = FileUtil.stat2Paths(fileStatus);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.filepaths = Arrays.asList(paths);
		bls = fbp.getBlocks(true, 128 * DataSamudayaConstants.MB, null);
		containers = new ConcurrentHashMap<>();
		tes = new ConcurrentHashMap<>();
		escontainer = Executors.newFixedThreadPool(100);
		var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
		var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
		var containeridports = new ConcurrentHashMap<String, List<Integer>>();
		ConcurrentMap<String, Resources> noderesourcesmap = new ConcurrentHashMap<>();
		zo = new ZookeeperOperations();
		DataSamudayaNodesResources.put(noderesourcesmap);
		for (; nodeindex < NOOFNODES; nodeindex++) {
			Resources resource = new Resources();
			int memory = 64;
			resource.setFreememory(memory * 1024 * 1024 * 1024l);
			resource.setNumberofprocessors(4);
			noderesourcesmap.put("127.0.0.1_" + (20000 + nodeindex), resource);
			server = Utils.getRPCRegistry(20000+nodeindex,
					new StreamDataCruncher() {
				public Object postObject(Object object)throws RemoteException {
						try {
							var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER,
									containerprocesses, hdfs, containeridthreads, containeridports,
									object, zo);
							Future<Object> containerallocated = escontainer.submit(container);
							Object returnresultobject = containerallocated.get();
							log.info("Containers Allocated: " + returnresultobject);
							return returnresultobject;
						} catch (InterruptedException e) {
							log.warn("Interrupted!", e);
							// Restore interrupted state...
							Thread.currentThread().interrupt();
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						return null;
					}
				}, DataSamudayaConstants.EMPTY);
			containerlauncher.add(server);
		}
	}

	@AfterClass
	public static void shutdownNodes() throws Exception {
		containers.keySet().stream().flatMap(key -> containers.get(key).stream()).forEach(servers -> {
			try {
				servers.close();
			} catch (IOException e) {
			}
		});
		tes.keySet().stream().flatMap(key -> tes.get(key).stream()).forEach(thr -> thr.stop());
		if (!Objects.isNull(escontainer)) {
			escontainer.shutdown();
		}
	}

	@Test
	public void testGetNodesResourcesSortedAuto() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetNodesResourcesSortedAuto() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.isblocksuserdefined = false;
		fbp.job = new Job();
		Map<String, Long> nodestotalblockmem = new ConcurrentHashMap<>();
		fbp.getDnXref(bls, false);
		fbp.getNodesResourcesSorted(bls, nodestotalblockmem);
		log.info(fbp.nodessorted);
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetNodesResourcesSortedAuto() Exiting------------------------------");
	}


	@Test
	public void testGetTaskExecutorsAuto() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsAuto() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.hdfs = hdfs;
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.job.setId(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		fbp.job.setJm(new JobMetrics());
		fbp.isignite = false;
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		assertEquals(Integer.valueOf(5), Integer.valueOf(fbp.job.getNodes().size()));
		assertEquals(Integer.valueOf(5), Integer.valueOf(fbp.job.getTaskexecutors().size()));
		fbp.destroyTaskExecutors();
		GlobalContainerAllocDealloc.getHportcrs().clear();
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsAuto() Exiting------------------------------");
	}

	@Test
	public void testGetTaskExecutorsProperInput() throws Exception {
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsProperInput() Entered------------------------------");
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.pipelineconfig = new PipelineConfig();
		fbp.pipelineconfig.setMaxmem("4096");
		fbp.pipelineconfig.setNumberofcontainers("5");
		fbp.job = new Job();
		fbp.job.setId(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		fbp.job.setJm(new JobMetrics());
		fbp.isignite = false;
		fbp.filepaths = Arrays.asList(paths);
		fbp.isblocksuserdefined = false;
		fbp.hdfs = hdfs;
		fbp.getDnXref(bls, false);
		fbp.allocateContainersByResources(bls);
		assertEquals(Integer.valueOf(5), Integer.valueOf(fbp.job.getNodes().size()));
		assertEquals(Integer.valueOf(5), Integer.valueOf(fbp.job.getTaskexecutors().size()));
		fbp.destroyTaskExecutors();
		GlobalContainerAllocDealloc.getHportcrs().clear();
		log.info("FileBlocksPartitionerHDFSMultipleNodesTest.testGetTaskExecutorsProperInput() Exiting------------------------------");
	}
}
