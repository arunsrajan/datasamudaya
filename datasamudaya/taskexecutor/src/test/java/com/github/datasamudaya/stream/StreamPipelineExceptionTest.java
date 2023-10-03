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

import java.util.List;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.stream.StreamPipeline;

public class StreamPipelineExceptionTest extends StreamPipelineBaseException {


	boolean toexecute = true;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValues() throws Throwable {
		log.info("testMapValuesReduceByValues Before---------------------------------------");

		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}

}
