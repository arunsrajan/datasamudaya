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
package com.github.datasamudaya.stream;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.rmi.NotBoundException;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.ByteBufferPoolDirectOld;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HadoopTestUtilities;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.TaskExecutorShutdown;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.tasks.executor.NodeRunner;

public class StreamPipelineBaseTestCommon extends StreamPipelineBase {
	static {
		System.setProperty("log4j.configurationFile", 
				System.getProperty(DataSamudayaConstants.USERDIR) + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J2_TEST_PROPERTIES);
	}
	static Registry server;
	static org.apache.logging.log4j.Logger log = LogManager.getLogger(StreamPipelineBaseTestCommon.class);
	protected static ZookeeperOperations zo;
	protected static String tejobid;

	@SuppressWarnings({"unused"})
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			if (!setupdone) {
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			}
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
			StaticComponentContainer.Modules.exportAllToAll();
			String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);			
			var out = System.out;
			pipelineconfig = new PipelineConfig();
			pipelineconfig.setOutput(out);
			pipelineconfig.setMaxmem("1024");
			pipelineconfig.setMinmem("512");
			pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
			pipelineconfig.setNumberofcontainers("1");
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
			pipelineconfig.setBatchsize("4");
			tejobid = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID();
			pipelineconfig.setTejobid(tejobid);
			pipelineconfig.setUser("arun");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
			ByteBufferPoolDirectOld.init(1 * DataSamudayaConstants.GB);
			ByteBufferPoolDirect.init(1 * DataSamudayaConstants.GB);
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
							DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.CACHEBLOCKS);
			CacheUtils.initBlockMetadataCache(DataSamudayaConstants.BLOCKCACHE);
			hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9000, 9870, 1);
			pipelineconfig.setBlocksize("20");
			pipelineconfig.setStorage(STORAGE.INMEMORY);
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			zo = new ZookeeperOperations();
			zo.connect();
			zo.watchNodes();
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			Boolean islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
			if (numberofnodes > 0) {
				host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskschedulerstream.host"));
				port = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskschedulerstream.port"));
				int nodeport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
				threadpool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				executorpool = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				port = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskexecutor.port"));
				int executorsindex = 0;
				Resources resource = new Resources();
				resource.setNodeport(host + DataSamudayaConstants.UNDERSCORE + nodeport);
				resource.setFreememory(4 * 1024 * 1024 * 1024l);
				resource.setFreememory(Utils.getTotalAvailablePhysicalMemory());
				resource.setNumberofprocessors(Utils.getAvailableProcessors());
				resource.setTotaldisksize(Utils.totaldiskspace());
				resource.setUsabledisksize(Utils.usablediskspace());
				resource.setPhysicalmemorysize(Utils.getPhysicalMemory());
				ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
				mapres.put(host + DataSamudayaConstants.UNDERSCORE + nodeport, resource);
				DataSamudayaNodesResources.put(mapres);
				zo.createNodesNode(host + DataSamudayaConstants.UNDERSCORE + nodeport, resource, event -> {
					log.info("{}", event);
				});
				while (isNull(DataSamudayaNodesResources.get()) || nonNull(DataSamudayaNodesResources.get()) && DataSamudayaNodesResources.get().size() != numberofnodes) {
					Thread.sleep(1000);
				}
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
						configuration);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {

					host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskexecutor.host"));
					if (isNull(server)) {
						StreamDataCruncher rpc = new StreamDataCruncher() {
							public Object postObject(Object object) throws RemoteException {
								try {
									if (object instanceof byte[] bytes) {
										object = Utils.convertBytesToObjectCompressed(bytes, null);
									}
									log.info("Allocate Or Launch: object {}", object);
									var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER,
											containerprocesses, hdfs, containeridthreads, containeridports, object, zo);
									Future<Object> containerallocated = threadpool.submit(container);
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
						};
						server = Utils.getRPCRegistry(nodeport, rpc, DataSamudayaConstants.EMPTY);
						sss.add(server);
					}
					port += 100;
					executorsindex++;
				}
			}
			Utils.uploadfile(hdfs, airlinesamplecsv, airlinesamplecsv + csvfileextn);
			Utils.uploadfile(hdfs, airportssample, airportssample + csvfileextn);
			Utils.uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
			Utils.uploadfile(hdfs, airlinesamplesql, airlinesamplesql + csvfileextn);
			Utils.uploadfile(hdfs, airlinesamplesqlucs, airlinesamplesqlucs + csvfileextn);
			Utils.uploadfile(hdfs, airlinesamplejoin, airlinesamplejoin + csvfileextn);
			Utils.uploadfile(hdfs, carriers, carriers + csvfileextn);
			Utils.uploadfile(hdfs, airline1987, airline1987 + csvfileextn);
			Utils.uploadfile(hdfs, bicyclecrash, bicyclecrash + csvfileextn);
			Utils.uploadfile(hdfs, population, population + csvfileextn);
			Utils.uploadfile(hdfs, airlinepairjoin, airlinepairjoin + csvfileextn);
			Utils.uploadfile(hdfs, airlinenoheader, airlinenoheader + csvfileextn);
			Utils.uploadfile(hdfs, airlinesamplenoheader, airlinesamplenoheader + csvfileextn);
			Utils.uploadfile(hdfs, cars, cars + txtfileextn);
			Utils.uploadfile(hdfs, wordcount, wordcount + txtfileextn);
			Utils.uploadfile(hdfs, airlinemultiplefilesfolder, airlinesample + csvfileextn);
			Utils.uploadfile(hdfs, airlinemultiplefilesfolder, airlinenoheader + csvfileextn);
			Utils.uploadfile(hdfs, githubevents, githubevents + jsonfileextn);

		} catch (Throwable e) {
			log.info("Error Uploading file", e);
		}
		setupdone = true;
	}

	

	@AfterClass
	public static void closeResources() throws Exception {
		if (nonNull(DataSamudayaCacheManager.get())) {
			DataSamudayaCacheManager.get().close();
			DataSamudayaCacheManager.put(null);
		}
		if (!Objects.isNull(hdfste)) {
			hdfste.close();
		}
		if (!Objects.isNull(hdfs)) {
			hdfs.close();
		}
		if (hdfsLocalCluster != null) {
			hdfsLocalCluster.shutdown(true);
		}
		if (executorpool != null) {
			executorpool.shutdown();
		}
		if (zo != null) {
			zo.close();
		}
		if (threadpool != null) {
			threadpool.shutdown();
		}
		if (testingserver != null) {
			testingserver.close();
		}

		if (CollectionUtils.isNotEmpty(sss)) {
			sss.stream().forEach(server -> {
				try {
					server.unbind(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + DataSamudayaConstants.EMPTY);
					UnicastRemoteObject.unexportObject(server, true);
				} catch (RemoteException | NotBoundException e) {
					log.info("Cannot Unbind: " + DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + DataSamudayaConstants.EMPTY);
				}
			});
			sss.clear();
			server = null;
		}

		containerprocesses.keySet().stream().forEach(key -> {
			containerprocesses.get(key).keySet().stream().forEach(port -> {
				Process proc = containerprocesses.get(key).get(port);
				if (Objects.nonNull(proc)) {
					log.info("In DC else Destroying the Container Process: " + proc);
					try {
						TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();
						log.info("Destroying the TaskExecutor: "
								+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
								+ DataSamudayaConstants.UNDERSCORE + port);
						Utils.getResultObjectByInput(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
								+ DataSamudayaConstants.UNDERSCORE + port, taskExecutorshutdown, key);
						log.info("Checking the Process is Alive for: "
								+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
								+ DataSamudayaConstants.UNDERSCORE + port);
						while (proc.isAlive()) {
							log.info("Destroying the TaskExecutor: "
									+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
									+ DataSamudayaConstants.UNDERSCORE + port);
							Thread.sleep(500);
						}
						log.info("Process Destroyed: " + proc + " for the port " + port);
					} catch (Exception ex) {
						log.error("Destroy failed for the process: " + proc);
					}
				}
			});
		});
		ByteBufferPoolDirectOld.destroy();
		ByteBufferPoolDirect.destroyPool();
	}
}
