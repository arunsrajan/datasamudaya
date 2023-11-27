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
package com.github.datasamudaya.stream.transformation.examples;

import java.io.Serializable;
import java.net.URI;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.Pipeline;
import com.github.datasamudaya.stream.StreamPipeline;

public class StreamReduceIntersection implements Serializable, Pipeline {
	private static final long serialVersionUID = -7001849661976107123L;
	private final Logger log = Logger.getLogger(StreamReduceIntersection.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_NORMAL);
		testReduce(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("StreamReduceIntersection.testReduce Before---------------------------------------");
		var datastream1 = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		var mappair1 = datastream1.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var airlinesample1 = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		var datastream2 = StreamPipeline.newStreamHDFS(args[0], args[2], pipelineconfig);
		var mappair2 = datastream2.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var airlinesample2 = mappair2.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);
		airlinesample1.intersection(airlinesample2)
				.saveAsTextFile(new URI(args[0]), args[3] + "/StreamOutReduce-" + System.currentTimeMillis());
		log.info("StreamReduceIntersection.testReduce After---------------------------------------");
	}
}
