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

import static org.junit.Assert.assertNotNull;

import java.io.FileInputStream;
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.utils.Utils;

public class LaunchContainersTest extends StreamPipelineBaseTestCommon {

	static {
		System.setProperty("log4j.configurationFile", 
				System.getenv(DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J2_PROPERTIES));
	}
	
	private final Logger log = LoggerFactory.getLogger(LaunchContainersTest.class);

	@BeforeClass
	public static void setup() throws Exception {
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
	}


	@Test
	public void testLaunchContainersDestroy() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("256");
		pc.setMinmem("256");
		pc.setLocal("false");
		pc.setJgroups("false");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setIsremotescheduler(false);
		pc.setOutput(System.out);
		pc.setBlocksize("64");
		pc.setUseglobaltaskexecutors(true);
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		pc.setTejobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		pc.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		mapres.put("127.0.0.1_12121", resource);
		DataSamudayaNodesResources.put(mapres);
		List<LaunchContainers> lcs = Utils.launchContainersExecutorSpecWithDriverSpec("arun", pc.getTejobid(), 1, 512, 1, 0, 0, true);
		assertNotNull(lcs);
		Utils.destroyContainers("arun", pc.getTejobid());
	}

	@Test
	public void testTELauncherJobSubmit() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("256");
		pc.setMinmem("256");
		pc.setJgroups("false");
		pc.setUser("arun");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setOutput(System.out);
		pc.setUseglobaltaskexecutors(true);
		pc.setIsremotescheduler(false);
		pc.setBlocksize("64");
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		Resources resource = new Resources();
		resource.setFreememory(12 * 1024 * 1024 * 1024l);
		resource.setNumberofprocessors(4);
		mapres.put("127.0.0.1_12121", resource);
		DataSamudayaNodesResources.put(mapres);
		ByteBufferPoolDirect.init(512 * DataSamudayaConstants.MB);
		pc.setLocal("false");
		pc.setUseglobaltaskexecutors(true);
		pc.setTejobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		pc.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		List<LaunchContainers> lcs = Utils.launchContainersExecutorSpecWithDriverSpec("arun", pc.getTejobid(), 1, 512, 1, 0, 0, true);
		assertNotNull(lcs);
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS("hdfs://127.0.0.1:9000", airlinesample, pc);
		List<List<Tuple2>> joinresult = (List) datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).collect(true, null);
		joinresult.stream().forEach(result -> log.info("{}", result));
		pc.setTejobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		pc.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		lcs = Utils.launchContainersExecutorSpecWithDriverSpec("arun", pc.getTejobid(), 1, 512, 1, 0, 0, true);
		assertNotNull(lcs);
		datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).saveAsTextFile(new URI("hdfs://127.0.0.1:9000"), "/Coalesce/Coalesce-" + System.currentTimeMillis());
		pc.setTejobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		pc.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis() + DataSamudayaConstants.HYPHEN + Utils.getUniqueJobID());
		lcs = Utils.launchContainersExecutorSpecWithDriverSpec("arun", pc.getTejobid(), 1, 512, 1, 0, 0, true);
		assertNotNull(lcs);
		MapPair<String, Tuple2<Long, Long>> mstll = datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2));
		joinresult = mstll.collect(true, null);
		joinresult.stream().forEach(result -> log.info("{}", result));
	}


}
