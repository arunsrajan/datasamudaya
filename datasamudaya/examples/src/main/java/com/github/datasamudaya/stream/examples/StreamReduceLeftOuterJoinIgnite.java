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

import java.net.URI;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.Pipeline;

public class StreamReduceLeftOuterJoinIgnite implements Pipeline {
	private final Logger log = Logger.getLogger(StreamReduceLeftOuterJoinIgnite.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		testMassiveDataCollectExampleLeftOuterJoin(args, pipelineconfig);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMassiveDataCollectExampleLeftOuterJoin(String[] args, PipelineConfig pipelineconfig)
			throws Exception {
		log.info("StreamReduceLeftOuterJoinIgnite.testMassiveDataCollectExampleLeftOuterJoin Before---------------------------------------");
		var datastream = StreamPipeline.newStreamHDFS(args[0], args[1], pipelineconfig);
		var mappair1 =  datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		var datastream1 = StreamPipeline.newStreamHDFS(args[0], args[2], pipelineconfig);

		var carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers.leftOuterjoin(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.saveAsTextFile(new URI(args[0]), args[3] + "/MapRed-" + System.currentTimeMillis());
		log.info("StreamReduceLeftOuterJoinIgnite.testMassiveDataCollectExampleLeftOuterJoin After---------------------------------------");
	}
}
