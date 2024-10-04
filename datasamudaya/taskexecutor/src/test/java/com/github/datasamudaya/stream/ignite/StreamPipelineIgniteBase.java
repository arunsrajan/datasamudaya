/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.stream.ignite;

import java.io.InputStream;
import java.net.URI;
import java.net.URL;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;

import org.apache.commons.io.IOUtils;
import org.apache.curator.test.TestingServer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.log4j.Logger;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.DataSamudayaIgniteServer;
import com.github.datasamudaya.common.utils.HadoopTestUtilities;
import com.github.datasamudaya.common.utils.Utils;

public class StreamPipelineIgniteBase {
	static MiniDFSCluster hdfsLocalCluster;
	String[] airlineheader = new String[]{"Year", "Month", "DayofMonth", "DayOfWeek", "DepTime",
			"CRSDepTime", "ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum",
			"ActualElapsedTime", "CRSElapsedTime", "AirTime", "ArrDelay", "DepDelay", "Origin", "Dest",
			"Distance", "TaxiIn", "TaxiOut", "Cancelled", "CancellationCode", "Diverted", "CarrierDelay",
			"WeatherDelay", "NASDelay", "SecurityDelay", "LateAircraftDelay"};
	String[] carrierheader = {"Code", "Description"};
	static String hdfsfilepath = "hdfs://127.0.0.1:9000";
	String airlines = "/airlines";
	String airline = "/airline";
	static String airportssample = "/airports";
	static String airlinenoheader = "/airlinenoheader";
	static String airlinesamplenoheader = "/airlinesamplenoheader";
	String airlineverysmall = "/airlineverysmall";
	String airlineveryverysmall1988 = "/airlineververysmall1988";
	String airlineveryverysmall1989 = "/airlineveryverysmall1989";
	String airlinepartitionsmall = "/airlinepartitionsmall";
	String airlinesmall = "/airlinesmall";
	static String airlinesamplesqlucs = "/airlinesamplesqlucs";
	static String airlinesample = "/airlinesample";
	static String airlinesamplesql = "/airlinesamplesql";
	static String airlinesamplejoin = "/airlinesamplejoin";
	static String csvfileextn = ".csv";
	static String txtfileextn = ".txt";
	static String jsonfileextn = ".json";
	String airline2 = "/airline2";
	static String airline1987 = "/airline1987";
	static String airports = "/airports";
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
	static int zookeeperport = 2181;
	static int namenodeport = 9000;
	static int namenodehttpport = 60070;
	static ExecutorService threadpool, executorpool;
	static int numberofnodes = 1;
	static Integer port;
	static FileSystem hdfs;
	static ConcurrentMap<String, List<Process>> containerprocesses = new ConcurrentHashMap<>();
	protected static PipelineConfig pipelineconfig = new PipelineConfig();
	static Logger log = Logger.getLogger(StreamPipelineIgniteBase.class);
	private static TestingServer testingserver;
	static Ignite igniteserver;
	static IgniteCache<Object, byte[]> ignitecache;

	@SuppressWarnings({"unused"})
	@BeforeClass
	public static void setServerUp() throws Exception {
		try {
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
			try {
				StaticComponentContainer.Modules.exportAllToAll();
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
			ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
			testingserver = new TestingServer(zookeeperport);
			testingserver.start();
			igniteserver = DataSamudayaIgniteServer.instance();
			ignitecache = igniteserver.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH, DataSamudayaConstants.CACHEDISKPATH_DEFAULT)
							+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.CACHEBLOCKS);
			CacheUtils.initBlockMetadataCache(DataSamudayaConstants.BLOCKCACHE);
			try {
				System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
				hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9000, 0, 1);
				URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			} catch (Throwable e) {
				log.error("Error In creating hdfs cluster:", e);
			}
			pipelineconfig.setLocal("false");
			pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
			Configuration configuration = new Configuration();
			hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			uploadfile(hdfs, airlinesamplesqlucs, airlinesamplesqlucs + csvfileextn);
			uploadfile(hdfs, airports, airports + csvfileextn);
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
	}

	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = StreamPipelineIgniteBase.class.getResourceAsStream(filename);
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
		if (Objects.nonNull(igniteserver)) {
			igniteserver.close();
			igniteserver = null;
		}
		if (Objects.nonNull(ignitecache)) {
			ignitecache.close();
			ignitecache = null;
		}
		if (Objects.nonNull(testingserver)) {
			testingserver.close();
		}
		if (Objects.nonNull(hdfs)) {
			hdfs.close();
			hdfs = null;
		}
		if (hdfsLocalCluster != null) {
			hdfsLocalCluster.shutdown(true);
			hdfsLocalCluster.close();
			hdfsLocalCluster = null;
		}
		if (executorpool != null) {
			executorpool.shutdown();
			executorpool = null;
		}
		if (threadpool != null) {
			threadpool.shutdown();
			threadpool = null;
		}
	}
}
