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

import java.io.InputStream;
import java.net.URI;
import java.rmi.registry.Registry;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.tasks.executor.NodeRunner;

public class StreamPipelineBaseException {
	static MiniDFSCluster hdfsLocalCluster;
	String[] airlineheader = new String[]{"Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	String airlines = "/airlines";
	String airline = "/airline";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	static String airlinesample = "/airlinesample";
	static String airlinesamplesql = "/airlinesamplesql";
	static String airlinesamplejoin = "/airlinesamplejoin";
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
	static int zookeeperport = 2182;
	static int namenodeport = 9000;
	static int namenodehttpport = 60070;
	private static String host;
	static Logger log = Logger.getLogger(StreamPipelineBaseException.class);
	static List<Registry> sss = new ArrayList<>();
	static ExecutorService threadpool, executorpool;
	static int numberofnodes = 1;
	static Integer port;
	static FileSystem hdfs;
	static boolean setupdone;
	static TestingServer testingserver;
	static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
	static FileSystem hdfste;
	protected static PipelineConfig pipelineconfig = new PipelineConfig();
	private static Registry server;
	private static ZookeeperOperations zo;
	
	@SuppressWarnings({"unused"})
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			var out = System.out;
			pipelineconfig.setOutput(out);
			pipelineconfig.setMaxmem("1024");
			pipelineconfig.setMinmem("512");
			pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
			pipelineconfig.setNumberofcontainers("3");
			pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);			
			System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatestexception.properties");
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
			                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
				            + DataSamudayaConstants.CACHEBLOCKS);
			pipelineconfig.setBlocksize("20");
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			zo = new ZookeeperOperations();
			zo.connect();
			Boolean ishdfs = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty("taskexecutor.ishdfs"));
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			Boolean islocal = Boolean.parseBoolean(pipelineconfig.getLocal());
			if (numberofnodes > 0) {
				int rescheduledelay = Integer
						.parseInt(DataSamudayaProperties.get().getProperty("taskschedulerstream.rescheduledelay"));
				int initialdelay = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskschedulerstream.initialdelay"));
				int pingdelay = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskschedulerstream.pingdelay"));
				host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskschedulerstream.host"));
				port =  Integer.parseInt(DataSamudayaProperties.get().getProperty("taskschedulerstream.port"));
				int nodeport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
				threadpool = Executors.newWorkStealingPool();
				executorpool = Executors.newWorkStealingPool();
				ClassLoader cl = Thread.currentThread().getContextClassLoader();
				port = Integer.parseInt(DataSamudayaProperties.get().getProperty("taskexecutor.port"));
				int executorsindex = 0;
				CountDownLatch cdl = new CountDownLatch(numberofnodes);
				ConcurrentMap<String, Map<String, Process>> containerprocesses = new ConcurrentHashMap<>();
				ConcurrentMap<String, Map<String, List<Thread>>> containeridthreads = new ConcurrentHashMap<>();
				hdfste = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
						configuration);
				var containeridports = new ConcurrentHashMap<String, List<Integer>>();
				while (executorsindex < numberofnodes) {
					host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskexecutor.host"));
					server = Utils.getRPCRegistry(nodeport,
							new StreamDataCruncher() {
				            public Object postObject(Object object) {
								try {
									var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER,
											containerprocesses, hdfs, containeridthreads, containeridports,
											object, zo);
									Future<Object> containerallocated = threadpool.submit(container);
									Object returnobject = containerallocated.get();
									log.info("Containers Allocated: " + returnobject);
									return returnobject;
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
					sss.add(server);
					port += 100;
					executorsindex++;
				}
			}
			uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
			uploadfile(hdfs, airlinesamplesql, airlinesamplesql + csvfileextn);
			uploadfile(hdfs, airlinesamplejoin, airlinesamplejoin + csvfileextn);
			uploadfile(hdfs, carriers, carriers + csvfileextn);
			uploadfile(hdfs, airline1987, airline1987 + csvfileextn);
			uploadfile(hdfs, bicyclecrash, bicyclecrash + csvfileextn);
			uploadfile(hdfs, population, population + csvfileextn);
			uploadfile(hdfs, airlinepairjoin, airlinepairjoin + csvfileextn);
			uploadfile(hdfs, airlinenoheader, airlinenoheader + csvfileextn);
			uploadfile(hdfs, airlinesamplenoheader, airlinesamplenoheader + csvfileextn);
			uploadfile(hdfs, cars, cars + txtfileextn);
			uploadfile(hdfs, wordcount, wordcount + txtfileextn);
			uploadfile(hdfs, airlinemultiplefilesfolder, airlinesample + csvfileextn);
			uploadfile(hdfs, airlinemultiplefilesfolder, airlinenoheader + csvfileextn);
			uploadfile(hdfs, githubevents, githubevents + jsonfileextn);


		} catch (Throwable e) {
			log.info("Error Uploading file", e);
		}
		setupdone = true;
	}

	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = StreamPipelineBaseException.class.getResourceAsStream(filename);
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


	@AfterClass
	public static void closeResources() throws Exception {
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
		if (threadpool != null) {
			threadpool.shutdown();
		}
		if (zo != null) {
			zo.close();
		}
		testingserver.close();
		DataSamudayaCacheManager.get().close();
		DataSamudayaCacheManager.put(null);
	}
}
