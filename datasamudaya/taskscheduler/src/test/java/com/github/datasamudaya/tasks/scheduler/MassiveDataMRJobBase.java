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

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.InputStream;
import java.net.URI;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.utils.HadoopTestUtilities;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.tasks.executor.NodeRunner;

public class MassiveDataMRJobBase {

	static Logger log = Logger.getLogger(MassiveDataMRJobBase.class);

	static MiniDFSCluster hdfsLocalCluster;
	static int namenodeport = 9000;
	static int namenodehttpport = 50070;
	static FileSystem hdfs;
	List<String> carrierheader = Arrays.asList("Code", "Description");
	List<SqlTypeName> carrierheadertypes = Arrays.asList(SqlTypeName.VARCHAR, SqlTypeName.VARCHAR);
	String hdfsfilepath = "hdfs://localhost:9000";
	String airlines = "/airlines";
	String airline1989 = "/airline1989";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	static String airlinesample = "/airlinesample";
	static String csvfileextn = ".csv";
	static String txtfileextn = ".txt";
	static String jsonfileextn = ".json";
	String airline2 = "/airline2";
	static String airline1987 = "/airline1987";
	String airlinemedium = "/airlinemedium";
	String airlineveryverysmall = "/airlineveryverysmall";
	String airlineveryveryverysmall = "/airlineveryveryverysmall";
	static String airlinepairjoin = "/airlinepairjoin";
	static String wordcount = "/wordcount";
	static String population = "/population";
	static String carriers = "/carriers";
	static String cars = "/cars";
	String groupbykey = "/groupbykey";
	static String bicyclecrash = "/bicyclecrash";
	static String airlinemultiplefilesfolder = "/airlinemultiplefilesfolder";
	static String githubevents = "/githubevents";
	private static int numberofnodes = 1;
	private static int port;
	static ExecutorService executorpool;
	static int zookeeperport = 2181;
	static boolean issetupdone = false;

	private static TestingServer testingserver;

	private static Registry server;

	private static ZookeeperOperations zo = null;
	
	@BeforeClass
	public static void setServerUp() throws Exception {		
		try (InputStream istream = MassiveDataMRJobBase.class.getResourceAsStream("/log4j.properties");) {
			System.setProperty("HIBCFG", "../config/datasamudayahibernate.cfg.xml");
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
			PropertyConfigurator.configure(istream);
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
			org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
			ByteBufferPoolDirect.init(2*DataSamudayaConstants.GB);
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE, 
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
			                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
				            + DataSamudayaConstants.CACHEBLOCKS);
			CacheUtils.initBlockMetadataCache(DataSamudayaConstants.BLOCKCACHE);
			hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9000, 9870, 2);
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			zo = new ZookeeperOperations();
			zo.connect();
			zo.watchNodes();
			executorpool = Executors.newWorkStealingPool();
			int rescheduledelay = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskscheduler.rescheduledelay"));
			int initialdelay = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskscheduler.initialdelay"));
			int pingdelay = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskscheduler.pingdelay"));
			String host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskscheduler.host"));
			port = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskscheduler.port"));
			Configuration configuration = new Configuration();
			hdfs = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty("hdfs.namenode.url")),
					configuration);
			log.info("HDFS FileSystem Object: " + hdfs);
			if (numberofnodes > 0 && !issetupdone) {
				port = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
				int executorsindex = 0;
				ConcurrentMap<String, Map<String, Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				ExecutorService es = Executors.newWorkStealingPool();
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				int nodeport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
				Resources resource = new Resources();
				resource.setNodeport(host + DataSamudayaConstants.UNDERSCORE + nodeport);
				resource.setTotalmemory(Runtime.getRuntime().totalMemory());
				resource.setFreememory(Utils.getTotalAvailablePhysicalMemory());
				resource.setNumberofprocessors(Utils.getAvailableProcessors());
				resource.setTotaldisksize(Utils.totaldiskspace());
				resource.setUsabledisksize(Utils.usablediskspace());
				resource.setPhysicalmemorysize(Utils.getPhysicalMemory());
				zo.createNodesNode(host+DataSamudayaConstants.UNDERSCORE+nodeport, resource, (event)->{
					log.info(event);
				});
				while(isNull(DataSamudayaNodesResources.get()) || nonNull(DataSamudayaNodesResources.get()) && DataSamudayaNodesResources.get().size()!=numberofnodes) {
					Thread.sleep(1000);
				}
				while (executorsindex < numberofnodes) {
					
					host = NetworkUtil
							.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
					issetupdone = true;
					server = Utils.getRPCRegistry(port,
							new StreamDataCruncher() {
						public Object postObject(Object object)throws RemoteException {
								try {
									var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER,
											containerprocesses, hdfs, containeridthreads, containeridports,
											object, zo);
									Future<Object> containerallocated = es.submit(container);
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
					port++;
					executorsindex++;
				}
			}

			uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
			uploadfile(hdfs, carriers, carriers + csvfileextn);
		} catch (Exception ex) {
			log.info("MRJobTestBase Initialization Error", ex);
		}
	}


	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Exception {
		InputStream is = MassiveDataMRJobBase.class.getResourceAsStream(filename);
		String jobpath = dir;
		String filepath = jobpath + filename;
		Path jobpathurl = new Path(jobpath);
		if (!hdfs.exists(jobpathurl)) {
			hdfs.mkdirs(jobpathurl);
		}
		Path filepathurl = new Path(filepath);
		FSDataOutputStream fsdos = hdfs.create(filepathurl);
		IOUtils.copy(is, fsdos);
		fsdos.hflush();
		is.close();
		fsdos.close();
	}

	public static void loadProperties(String filename) throws Exception {
		Properties prop = new Properties();
		InputStream fis = MassiveDataMRJobBase.class.getResourceAsStream(filename);
		prop.load(fis);
		fis.close();
		if (DataSamudayaProperties.get() != null) {
			DataSamudayaProperties.get().putAll(prop);
		} else {
			DataSamudayaProperties.put(prop);
		}
	}

	@AfterClass
	public static void closeResources() throws Exception {
		executorpool.shutdown();
		testingserver.stop();
		testingserver.close();
	}
}
