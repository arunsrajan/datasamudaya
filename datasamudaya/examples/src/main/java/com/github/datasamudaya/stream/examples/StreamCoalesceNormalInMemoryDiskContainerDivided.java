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
package com.github.datasamudaya.stream.examples;

import java.io.Serializable;
import java.net.URI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.StreamPipeline;

public class StreamCoalesceNormalInMemoryDiskContainerDivided implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private final Logger log = LogManager.getLogger(StreamCoalesceNormalInMemoryDiskContainerDivided.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);

		pipelineconfig.setBlocksize("128");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setMode(DataSamudayaConstants.NORMAL);
		pipelineconfig.setNumberofcontainers("3");
		pipelineconfig.setBatchsize(args[3]);
		pipelineconfig.setContaineralloc("DIVIDED");
		testReduce(args, pipelineconfig);
	}

	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info(
				"StreamCoalesceNormalInMemoryDiskContainerDivided.testReduce Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues(
						(tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.saveAsTextFile(new URI(args[0]), args[2] + "/StreamOutReduce-" + System.currentTimeMillis());
		log.info(
				"StreamCoalesceNormalInMemoryDiskContainerDivided.testReduce After---------------------------------------");
	}
}
