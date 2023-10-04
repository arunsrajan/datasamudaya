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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.List;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

public class StreamPipelineCoalesceTest extends StreamPipelineBaseTestCommon {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testCoalesce() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesce Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> coalesceresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2).collect(true, null);

		long sum = 0;
		for (List<Tuple> tuples : coalesceresult) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(-63278, sum);
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesce After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCoalesceWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesceWithJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> coalesce = (MapPair<String, Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		List<List<Tuple>> coalesceresult = coalesce1.join(coalesce2, (dat1, dat2) -> dat1.equals(dat2)).collect(true, null);

		long sum = 0;
		for (List<Tuple> tuples : coalesceresult) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) ((Tuple2) tuple).v2).v2;
			}
			log.info("");
		}
		assertEquals(-63278, sum);
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesceWithJoin After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduceByKeyCoalesceWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testReduceByKeyCoalesceWithJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> coalesce = (MapPair<String, Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		List<List<Tuple2<Tuple2<String, Object>, Tuple2<String, Object>>>> coalesceresult = coalesce1.join(coalesce2, (dat1, dat2) -> ((Tuple2) dat1).v1.equals(((Tuple2) dat2).v1)).collect(true, null);

		for (List<Tuple2<Tuple2<String, Object>, Tuple2<String, Object>>> tuples : coalesceresult) {
			for (Tuple2<Tuple2<String, Object>, Tuple2<String, Object>> tuple : tuples) {
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
				log.info(tuple);
			}
			log.info("");
		}
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testReduceByKeyCoalesceWithJoin After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCoalesceReduceByKeyWithJoin() throws Throwable {
		String pipelineconfigblocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		log.info("testCoalesceReduceByKeyWithJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Long> coalesce = (MapPair<String, Long>) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> coalesce1 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> coalesce2 = coalesce
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		List<List<Tuple2<Tuple2<String, Object>, Tuple2<String, Object>>>> coalesceresult = coalesce2.join(coalesce1, (dat1, dat2) -> ((Tuple2) dat1).v1.equals(((Tuple2) dat2).v1)).collect(true, null);

		for (List<Tuple2<Tuple2<String, Object>, Tuple2<String, Object>>> tuples : coalesceresult) {
			for (Tuple2<Tuple2<String, Object>, Tuple2<String, Object>> tuple : tuples) {
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
				log.info(tuple);
			}
			log.info("");
		}
		pipelineconfig.setBlocksize(pipelineconfigblocksize);

		log.info("testCoalesceReduceByKeyWithJoin After---------------------------------------");
	}
}
