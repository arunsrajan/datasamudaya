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

import static java.util.Objects.nonNull;
import java.io.InputStream;
import java.net.URI;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.apache.log4j.Logger;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.ehcache.Cache;
import org.junit.AfterClass;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.HadoopTestUtilities;
import com.github.datasamudaya.common.utils.Utils;

public class StreamPipelineTestCommon {
	public static Cache<String, byte[]> cache;
	protected static String STAR = "*";
	protected static int namenodeport = 9000;
	protected static int namenodehttpport = 50070;
	protected static MiniDFSCluster hdfsLocalCluster;
	protected static FileSystem hdfs;
	protected static String airlinesample = "/airlinesample";
	protected static String airlinesampleintersection = "/airlinesampleintersection";
	protected static String airlinesampleunion1 = "/airlinesampleunion1";
	protected static String airlinesampleunion2 = "/airlinesampleunion2";
	protected static String githubevents = "/githubevents";
	protected static String csvfileextn = ".csv";
	protected String[] hdfsdirpaths1 = {"/airlinesample"};
	protected String[] hdfsdirpaths2 = {"/airlinesampleintersection"};
	protected String[] hdfsdirpaths3 = {"/airlinesampleunion1"};
	protected String[] hdfsdirpaths4 = {"/airlinesampleunion2"};
	protected String[] githubevents1 = {"/githubevents"};
	protected static String jsonfileextn = ".json";
	protected  static String hdfsurl = "hdfs://127.0.0.1:9000";
	protected String[] airlineheader = new String[]{"Year", "Month", "DayofMonth", "DayOfWeek", "DepTime", "CRSDepTime",
			"ArrTime", "CRSArrTime", "UniqueCarrier", "FlightNum", "TailNum", "ActualElapsedTime", "CRSElapsedTime",
			"AirTime", "ArrDelay", "DepDelay", "Origin", "Dest", "Distance", "TaxiIn", "TaxiOut", "Cancelled",
			"CancellationCode", "Diverted", "CarrierDelay", "WeatherDelay", "NASDelay", "SecurityDelay",
			"LateAircraftDelay"};
	static Logger log = Logger.getLogger(StreamPipelineTestCommon.class);
	protected static ExecutorService es;

	@BeforeClass
	public static void init() {
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
	}

	@AfterClass
	public static void tearDown() throws InterruptedException {
		es.shutdownNow();
		es.awaitTermination(2000, TimeUnit.MILLISECONDS);
	}

	@BeforeClass
	public static void setUp() throws Throwable {
		StaticComponentContainer.Modules.exportAllToAll();
		System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
		Configuration conf = new Configuration();
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
		ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
		CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
	            + DataSamudayaConstants.CACHEBLOCKS);
		CacheUtils.initBlockMetadataCache(DataSamudayaConstants.BLOCKCACHE);
		hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9000, 9870, 2);
		cache = (Cache<String, byte[]>) DataSamudayaCache.get();
		hdfs = FileSystem.newInstance(new URI(hdfsurl),
				conf);
		uploadfile(hdfs, airlinesample, airlinesample + csvfileextn);
		uploadfile(hdfs, airlinesampleintersection, airlinesampleintersection + csvfileextn);
		uploadfile(hdfs, airlinesampleunion1, airlinesampleunion1 + csvfileextn);
		uploadfile(hdfs, airlinesampleunion2, airlinesampleunion2 + csvfileextn);
		uploadfile(hdfs, githubevents, githubevents + jsonfileextn);
	}

	public static void uploadfile(FileSystem hdfs, String dir, String filename) throws Throwable {
		InputStream is = StreamPipelineTestCommon.class.getResourceAsStream(filename);
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
	public static void closeResources() throws Throwable {
		if (!Objects.isNull(hdfsLocalCluster)) {
			hdfsLocalCluster.shutdown(true);
		}
		if (!Objects.isNull(hdfs)) {
			hdfs.close();
		}
		ByteBufferPoolDirect.destroy();
		if(nonNull(DataSamudayaCacheManager.get())){
			DataSamudayaCacheManager.get().close();
			DataSamudayaCacheManager.put(null);
		}
	}
}
