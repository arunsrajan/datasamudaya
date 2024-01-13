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
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.StreamPipeline;

public class StreamAggSumCountArrDelayInMemoryDivided implements Serializable, Pipeline {
	private static final long serialVersionUID = -1073668309871473457L;
	private final Logger log = Logger.getLogger(StreamAggSumCountArrDelayInMemoryDivided.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem(args[3]);
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setBatchsize(args[4]);
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		pipelineconfig.setContaineralloc(DataSamudayaConstants.CONTAINER_ALLOC_DIVIDED);
		testMapValuesReduceByValues(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMapValuesReduceByValues(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("testMapValuesReduceByValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.saveAsTextFile(new URI(args[0]), args[2] + "/StreamAggSumCount-" + System.currentTimeMillis());
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}
}
