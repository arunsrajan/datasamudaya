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
import java.net.URI;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.MapPair;
import com.github.datasamudaya.stream.StreamPipeline;

public class LaunchContainersTest extends StreamPipelineBaseTestCommon {

	@Test
	public void testLaunchContainersDestroy() throws Exception {
		PipelineConfig pc = new PipelineConfig();		
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("1024");
		pc.setMinmem("1024");
		pc.setLocal("true");
		pc.setJgroups("false");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setOutput(System.out);
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("64");
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		pc.setJobid(DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID());
		Resources resources = new Resources();
		resources.setNumberofprocessors(12);
		resources.setFreememory(4294967296l);
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		mapres.put("127.0.0.1_12121", resources);
		resources.setNodeport("127.0.0.1_12121");
		DataSamudayaNodesResources.put(mapres);
		List<LaunchContainers> lcs = Utils.launchContainers("arun", pc.getJobid());
		assertNotNull(lcs);
		Utils.destroyContainers("arun", pc.getJobid());
	}

	@Test
	public void testTELauncherJobSubmit() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		pc.setNumberofcontainers("1");
		pc.setMaxmem("512");
		pc.setMinmem("512");
		pc.setJgroups("false");
		pc.setMesos("false");
		pc.setYarn("false");
		pc.setOutput(System.out);
		pc.setIsblocksuserdefined("true");
		pc.setBlocksize("64");
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		Resources resources = new Resources();
		resources.setNumberofprocessors(12);
		resources.setFreememory(4294967296l);
		ConcurrentMap<String, Resources> mapres = new ConcurrentHashMap<>();
		mapres.put("127.0.0.1_12121", resources);
		resources.setNodeport("127.0.0.1_12121");
		DataSamudayaNodesResources.put(mapres);
		pc.setJobid(DataSamudayaConstants.JOB+DataSamudayaConstants.HYPHEN+System.currentTimeMillis()+DataSamudayaConstants.HYPHEN+Utils.getUniqueJobID());
		List<LaunchContainers> lcs = Utils.launchContainers("arun", pc.getJobid());
		assertNotNull(lcs);
		ByteBufferPoolDirect.init(2*DataSamudayaConstants.GB);
		pc.setLocal("false");
		pc.setUseglobaltaskexecutors(true);		
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS("hdfs://127.0.0.1:9000", "/airline1989", pc);
		List<List<Tuple2>> joinresult = (List) datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).collect(true, null);
		joinresult.stream().forEach(log::info);
		datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).saveAsTextFile(new URI("hdfs://127.0.0.1:9000"), "/Coalesce/Coalesce-" + System.currentTimeMillis());
		MapPair<String, Tuple2<Long, Long>> mstll = datastream.map(dat -> dat.split(",")).filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14]))).mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2)).coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2));
		joinresult = mstll.collect(true, null);
		joinresult.stream().forEach(log::info);
		Utils.destroyContainers("arun",pc.getJobid());
		pc.setLocal("true");
	}


}
