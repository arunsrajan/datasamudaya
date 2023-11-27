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

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.functions.HashPartitioner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipeline1Test extends StreamPipelineBaseTestCommon {

	boolean toexecute = true;
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testHashPartitionerReduceByKeyPartitioned() throws Throwable {
		log.info("testHashPartitionerReduceByKeyPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<Integer,List<Tuple2<String,Integer>>>>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToPair(str -> new Tuple2<String,Integer>(str[1], Integer.parseInt(str[14])))
				.partition(new HashPartitioner(3))
				.flatMap(tuples->tuples.v2().stream())
				.reduceByKey((a,b)->a+b)
				.partition(new HashPartitioner(3))
				.collect(toexecute, null);
		int sum = 0;
		assertEquals(1, tupleslist.size());
		for (List<Tuple2<Integer,List<Tuple2<String,Integer>>>> tuples : tupleslist) {			
			for (Tuple2<Integer,List<Tuple2<String,Integer>>> tuple2 : tuples) {
				log.info("partition-------");
				for(Tuple2<String,Integer> tup2:tuple2.v2()) {
					log.info(tup2);
					sum += tup2.v2();
				}
			}			
		}
		assertEquals(-63278l, sum);
		log.info("testHashPartitionerReduceByKeyPartitioned After---------------------------------------");
	}
	
	
	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testGroupBy() throws Throwable {
		log.info("testGroupBy Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<Map, List<String[]>>>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.groupBy(str->{
					Map<String, Object> map = new HashMap<>();
					map.put("MonthOfYear", str[1]);
					map.put("DayOfMonth", str[2]);
					map.put("UniqueCarrier", str[8]);
					return map;
				})
				.collect(toexecute, null);
		int sum = 0;
		assertEquals(1, tupleslist.size());
		for (List<Tuple2<Map, List<String[]>>> tuples : tupleslist) {			
			for (Tuple2<Map, List<String[]>> tuple2 : tuples) {
				log.info("partition-------");
				log.info(tuple2);
				sum += tuple2.v2.size();
			}			
		}
		assertEquals(45957l, sum);
		log.info("testGroupBy After---------------------------------------");
	}
}
