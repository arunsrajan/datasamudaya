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
package com.github.datasamudaya.stream.ignite;

import static org.junit.Assert.assertEquals;
import java.util.List;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.stream.IgnitePipeline;
import com.github.datasamudaya.stream.MapPairIgnite;

@SuppressWarnings({"rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IgnitePipelineFileTest  extends StreamPipelineIgniteBase {
	boolean toexecute = true;
	String airlinesfolder = "file:E:/DEVELOPMENT/dataset/airline";
	String carriersfolder = "file:E:/DEVELOPMENT/dataset/carriers";
	static Logger log = Logger.getLogger(IgnitePipelineFileTest.class);
	@SuppressWarnings("unchecked")
	@Test
	public void testMapFilterIgnite() throws Throwable {
		log.info("testMapFilterIgnite Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("false");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStream(airlinesfolder,
				pipelineconfig);
		IgnitePipeline<String[]> mdpi = datapipeline.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).cache(false);
		IgnitePipeline<Tuple2<String, Integer>> tupresult = mdpi.map(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(true);
		int sum = 0;
		List<List> results = (List) ((List) tupresult.job.getResults());
		sum = 0;
		for (List result :results) {
			sum += result.size();
		}
		assertEquals(1288326, sum);
		IgnitePipeline<Tuple2<String, Integer>> tupresult1 = mdpi.map(dat -> new Tuple2<String, Integer>(dat[0], Integer.parseInt(dat[14]))).cache(true);
		results = (List) ((List) tupresult1.job.getResults());
		sum = 0;
		for (List result :results) {
			sum += result.size();
		}
		assertEquals(1288326, sum);
		log.info("testMapFilterIgnite After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMapFilterMapPairRbkIgniteJoin() throws Throwable {
		log.info("testMapFilterMapPairRbkIgniteJoin Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setIsblocksuserdefined("false");
		IgnitePipeline<String> datastream = IgnitePipeline.newStream(airlinesfolder,
				pipelineconfig);
		MapPairIgnite<String, Integer> mti = datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> new Tuple2<String, Integer>(dat[8], Integer.parseInt(dat[14]))).cache(false);
		MapPairIgnite<String, Integer> tupresult = mti.reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult.job.getResults()).get(0)).size());
		MapPairIgnite<String, Integer> tupresult1 = mti.reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).cache(true);
		assertEquals(14, ((List) ((List) tupresult1.job.getResults()).get(0)).size());
		MapPairIgnite<Tuple2<String, Integer>, Tuple2<String, Integer>> joinresult = (MapPairIgnite) tupresult.join(tupresult1, (tup1, tup2) -> tup1.v1.equals(tup2.v1)).cache(true);
		assertEquals(14, ((List) ((List) joinresult.job.getResults()).get(0)).size());
		log.info("testMapFilterMapPairRbkIgniteJoin After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testReduceByKeyCoalesceJoinUserDefinedBlockSize() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("256");
		IgnitePipeline<String> datastream = IgnitePipeline.newStream(airlinesfolder,
				pipelineconfig);
		MapPairIgnite<String, Long> mappair1 = (MapPairIgnite) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPairIgnite<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		IgnitePipeline<String> datastream1 = IgnitePipeline.newStream(carriersfolder, pipelineconfig);

		MapPairIgnite<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> results = (List<List>) carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).collect(true, null);
		log.info(results);
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize After---------------------------------------");
	}
}
