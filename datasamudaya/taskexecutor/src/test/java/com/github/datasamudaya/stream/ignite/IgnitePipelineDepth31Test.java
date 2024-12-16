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

import java.util.Arrays;
import java.util.List;
import java.util.stream.Stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.functions.FlatMapFunction;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.stream.StreamPipeline;

@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IgnitePipelineDepth31Test extends StreamPipelineIgniteBase {
	boolean toexecute = true;
	int sum;
	static Logger log = LogManager.getLogger(IgnitePipelineDepth31Test.class);

	@Test
	public void testFilterFilterFilterCollect() throws Throwable {
		log.info("testFilterFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterFilter After---------------------------------------");
	}

	@Test
	public void testFilterFilterFilterCount() throws Throwable {
		log.info("testFilterFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterFilterForEach() throws Throwable {
		log.info("testFilterFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterFilter After---------------------------------------");
	}

	@Test
	public void testFilterFilterFlatMapCollect() throws Throwable {
		log.info("testFilterFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterFilterFlatMapCount() throws Throwable {
		log.info("testFilterFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterFlatMapForEach() throws Throwable {
		log.info("testFilterFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapCollect() throws Throwable {
		log.info("testFilterFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterMap After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapCount() throws Throwable {
		log.info("testFilterFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterMapCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapForEach() throws Throwable {
		log.info("testFilterFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterMap After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairCollect() throws Throwable {
		log.info("testFilterFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairCount() throws Throwable {
		log.info("testFilterFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairForEach() throws Throwable {
		log.info("testFilterFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterFilterMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterFilterPeekCollect() throws Throwable {
		log.info("testFilterFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterPeek After---------------------------------------");
	}

	@Test
	public void testFilterFilterPeekCount() throws Throwable {
		log.info("testFilterFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterPeekForEach() throws Throwable {
		log.info("testFilterFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterPeek After---------------------------------------");
	}

	@Test
	public void testFilterFilterSampleCollect() throws Throwable {
		log.info("testFilterFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterSample After---------------------------------------");
	}

	@Test
	public void testFilterFilterSampleCount() throws Throwable {
		log.info("testFilterFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterSampleForEach() throws Throwable {
		log.info("testFilterFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterSample After---------------------------------------");
	}

	@Test
	public void testFilterFilterSortedCollect() throws Throwable {
		log.info("testFilterFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterSorted After---------------------------------------");
	}

	@Test
	public void testFilterFilterSortedCount() throws Throwable {
		log.info("testFilterFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterSortedForEach() throws Throwable {
		log.info("testFilterFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilterSorted After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFilterCollect() throws Throwable {
		log.info("testFilterFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFilterCount() throws Throwable {
		log.info("testFilterFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFilterForEach() throws Throwable {
		log.info("testFilterFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFlatMapCollect() throws Throwable {
		log.info("testFilterFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFlatMapCount() throws Throwable {
		log.info("testFilterFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapFlatMapForEach() throws Throwable {
		log.info("testFilterFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapCollect() throws Throwable {
		log.info("testFilterFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapCount() throws Throwable {
		log.info("testFilterFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapForEach() throws Throwable {
		log.info("testFilterFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairCollect() throws Throwable {
		log.info("testFilterFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairCount() throws Throwable {
		log.info("testFilterFlatMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairForEach() throws Throwable {
		log.info("testFilterFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapPeekCollect() throws Throwable {
		log.info("testFilterFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapPeekCount() throws Throwable {
		log.info("testFilterFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapPeekForEach() throws Throwable {
		log.info("testFilterFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSampleCollect() throws Throwable {
		log.info("testFilterFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSampleCount() throws Throwable {
		log.info("testFilterFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSampleForEach() throws Throwable {
		log.info("testFilterFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSortedCollect() throws Throwable {
		log.info("testFilterFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSortedCount() throws Throwable {
		log.info("testFilterFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapSortedForEach() throws Throwable {
		log.info("testFilterFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testFilterMapFilterCollect() throws Throwable {
		log.info("testFilterMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapFilterCount() throws Throwable {
		log.info("testFilterMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterMapFilterForEach() throws Throwable {
		log.info("testFilterMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapFlatMapCollect() throws Throwable {
		log.info("testFilterMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapFlatMapCount() throws Throwable {
		log.info("testFilterMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapFlatMapForEach() throws Throwable {
		log.info("testFilterMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapMapCollect() throws Throwable {
		log.info("testFilterMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapMap After---------------------------------------");
	}

	@Test
	public void testFilterMapMapCount() throws Throwable {
		log.info("testFilterMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapMapForEach() throws Throwable {
		log.info("testFilterMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapMap After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairCollect() throws Throwable {
		log.info("testFilterMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairCount() throws Throwable {
		log.info("testFilterMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairForEach() throws Throwable {
		log.info("testFilterMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPeekCollect() throws Throwable {
		log.info("testFilterMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPeekCount() throws Throwable {
		log.info("testFilterMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPeekForEach() throws Throwable {
		log.info("testFilterMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapSampleCollect() throws Throwable {
		log.info("testFilterMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapSample After---------------------------------------");
	}

	@Test
	public void testFilterMapSampleCount() throws Throwable {
		log.info("testFilterMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterMapSampleForEach() throws Throwable {
		log.info("testFilterMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapSample After---------------------------------------");
	}

	@Test
	public void testFilterMapSortedCollect() throws Throwable {
		log.info("testFilterMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapSorted After---------------------------------------");
	}

	@Test
	public void testFilterMapSortedCount() throws Throwable {
		log.info("testFilterMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterMapSortedForEach() throws Throwable {
		log.info("testFilterMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapSorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFilterCollect() throws Throwable {
		log.info("testFilterMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFilterCount() throws Throwable {
		log.info("testFilterMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFilterForEach() throws Throwable {
		log.info("testFilterMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFlatMapCollect() throws Throwable {
		log.info("testFilterMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFlatMapCount() throws Throwable {
		log.info("testFilterMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairFlatMapForEach() throws Throwable {
		log.info("testFilterMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapCollect() throws Throwable {
		log.info("testFilterMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapCount() throws Throwable {
		log.info("testFilterMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapForEach() throws Throwable {
		log.info("testFilterMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairCollect() throws Throwable {
		log.info("testFilterMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairCount() throws Throwable {
		log.info("testFilterMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairForEach() throws Throwable {
		log.info("testFilterMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairPeekCollect() throws Throwable {
		log.info("testFilterMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairPeekCount() throws Throwable {
		log.info("testFilterMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairPeekForEach() throws Throwable {
		log.info("testFilterMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSampleCollect() throws Throwable {
		log.info("testFilterMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairSample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSampleCount() throws Throwable {
		log.info("testFilterMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSampleForEach() throws Throwable {
		log.info("testFilterMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairSample After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSortedCollect() throws Throwable {
		log.info("testFilterMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairSorted After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSortedCount() throws Throwable {
		log.info("testFilterMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairSortedForEach() throws Throwable {
		log.info("testFilterMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMapPairSorted After---------------------------------------");
	}

	@Test
	public void testFilterPeekFilterCollect() throws Throwable {
		log.info("testFilterPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekFilter After---------------------------------------");
	}

	@Test
	public void testFilterPeekFilterCount() throws Throwable {
		log.info("testFilterPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekFilterForEach() throws Throwable {
		log.info("testFilterPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekFilter After---------------------------------------");
	}

	@Test
	public void testFilterPeekFlatMapCollect() throws Throwable {
		log.info("testFilterPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterPeekFlatMapCount() throws Throwable {
		log.info("testFilterPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekFlatMapForEach() throws Throwable {
		log.info("testFilterPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapCollect() throws Throwable {
		log.info("testFilterPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekMap After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapCount() throws Throwable {
		log.info("testFilterPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekMapCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapForEach() throws Throwable {
		log.info("testFilterPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekMap After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairCollect() throws Throwable {
		log.info("testFilterPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekMapPair After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairCount() throws Throwable {
		log.info("testFilterPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairForEach() throws Throwable {
		log.info("testFilterPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekMapPair After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterPeekPeekCollect() throws Throwable {
		log.info("testFilterPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekPeek After---------------------------------------");
	}

	@Test
	public void testFilterPeekPeekCount() throws Throwable {
		log.info("testFilterPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekPeekForEach() throws Throwable {
		log.info("testFilterPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekPeek After---------------------------------------");
	}

	@Test
	public void testFilterPeekSampleCollect() throws Throwable {
		log.info("testFilterPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekSample After---------------------------------------");
	}

	@Test
	public void testFilterPeekSampleCount() throws Throwable {
		log.info("testFilterPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekSampleForEach() throws Throwable {
		log.info("testFilterPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekSample After---------------------------------------");
	}

	@Test
	public void testFilterPeekSortedCollect() throws Throwable {
		log.info("testFilterPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekSorted After---------------------------------------");
	}

	@Test
	public void testFilterPeekSortedCount() throws Throwable {
		log.info("testFilterPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekSortedForEach() throws Throwable {
		log.info("testFilterPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeekSorted After---------------------------------------");
	}

	@Test
	public void testFilterSampleFilterCollect() throws Throwable {
		log.info("testFilterSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleFilter After---------------------------------------");
	}

	@Test
	public void testFilterSampleFilterCount() throws Throwable {
		log.info("testFilterSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleFilterForEach() throws Throwable {
		log.info("testFilterSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleFilter After---------------------------------------");
	}

	@Test
	public void testFilterSampleFlatMapCollect() throws Throwable {
		log.info("testFilterSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterSampleFlatMapCount() throws Throwable {
		log.info("testFilterSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleFlatMapForEach() throws Throwable {
		log.info("testFilterSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapCollect() throws Throwable {
		log.info("testFilterSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleMap After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapCount() throws Throwable {
		log.info("testFilterSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleMapCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapForEach() throws Throwable {
		log.info("testFilterSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleMap After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairCollect() throws Throwable {
		log.info("testFilterSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleMapPair After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairCount() throws Throwable {
		log.info("testFilterSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairForEach() throws Throwable {
		log.info("testFilterSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleMapPair After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterSamplePeekCollect() throws Throwable {
		log.info("testFilterSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSamplePeek After---------------------------------------");
	}

	@Test
	public void testFilterSamplePeekCount() throws Throwable {
		log.info("testFilterSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testFilterSamplePeekForEach() throws Throwable {
		log.info("testFilterSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSamplePeek After---------------------------------------");
	}

	@Test
	public void testFilterSampleSampleCollect() throws Throwable {
		log.info("testFilterSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleSample After---------------------------------------");
	}

	@Test
	public void testFilterSampleSampleCount() throws Throwable {
		log.info("testFilterSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleSampleForEach() throws Throwable {
		log.info("testFilterSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleSample After---------------------------------------");
	}

	@Test
	public void testFilterSampleSortedCollect() throws Throwable {
		log.info("testFilterSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleSorted After---------------------------------------");
	}

	@Test
	public void testFilterSampleSortedCount() throws Throwable {
		log.info("testFilterSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleSortedForEach() throws Throwable {
		log.info("testFilterSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSampleSorted After---------------------------------------");
	}

	@Test
	public void testFilterSortedFilterCollect() throws Throwable {
		log.info("testFilterSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedFilter After---------------------------------------");
	}

	@Test
	public void testFilterSortedFilterCount() throws Throwable {
		log.info("testFilterSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedFilterForEach() throws Throwable {
		log.info("testFilterSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedFilter After---------------------------------------");
	}

	@Test
	public void testFilterSortedFlatMapCollect() throws Throwable {
		log.info("testFilterSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterSortedFlatMapCount() throws Throwable {
		log.info("testFilterSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedFlatMapForEach() throws Throwable {
		log.info("testFilterSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapCollect() throws Throwable {
		log.info("testFilterSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedMap After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapCount() throws Throwable {
		log.info("testFilterSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedMapCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapForEach() throws Throwable {
		log.info("testFilterSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedMap After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairCollect() throws Throwable {
		log.info("testFilterSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedMapPair After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairCount() throws Throwable {
		log.info("testFilterSortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairForEach() throws Throwable {
		log.info("testFilterSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedMapPair After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterSortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFilterSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFilterSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterSortedPeekCollect() throws Throwable {
		log.info("testFilterSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedPeek After---------------------------------------");
	}

	@Test
	public void testFilterSortedPeekCount() throws Throwable {
		log.info("testFilterSortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedPeekForEach() throws Throwable {
		log.info("testFilterSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedPeek After---------------------------------------");
	}

	@Test
	public void testFilterSortedSampleCollect() throws Throwable {
		log.info("testFilterSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedSample After---------------------------------------");
	}

	@Test
	public void testFilterSortedSampleCount() throws Throwable {
		log.info("testFilterSortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedSampleForEach() throws Throwable {
		log.info("testFilterSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedSample After---------------------------------------");
	}

	@Test
	public void testFilterSortedSortedCollect() throws Throwable {
		log.info("testFilterSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedSorted After---------------------------------------");
	}

	@Test
	public void testFilterSortedSortedCount() throws Throwable {
		log.info("testFilterSortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFilterSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedSortedForEach() throws Throwable {
		log.info("testFilterSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSortedSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFilterCollect() throws Throwable {
		log.info("testFlatMapFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFilterCount() throws Throwable {
		log.info("testFlatMapFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFilterForEach() throws Throwable {
		log.info("testFlatMapFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFlatMapCollect() throws Throwable {
		log.info("testFlatMapFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFlatMapCount() throws Throwable {
		log.info("testFlatMapFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterFlatMapForEach() throws Throwable {
		log.info("testFlatMapFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapCollect() throws Throwable {
		log.info("testFlatMapFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapCount() throws Throwable {
		log.info("testFlatMapFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapForEach() throws Throwable {
		log.info("testFlatMapFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairCollect() throws Throwable {
		log.info("testFlatMapFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairCount() throws Throwable {
		log.info("testFlatMapFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairForEach() throws Throwable {
		log.info("testFlatMapFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFlatMapFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapFilterMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testFlatMapFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testFlatMapFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterPeekCollect() throws Throwable {
		log.info("testFlatMapFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterPeekCount() throws Throwable {
		log.info("testFlatMapFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterPeekForEach() throws Throwable {
		log.info("testFlatMapFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSampleCollect() throws Throwable {
		log.info("testFlatMapFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterSample After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSampleCount() throws Throwable {
		log.info("testFlatMapFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSampleForEach() throws Throwable {
		log.info("testFlatMapFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterSample After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSortedCollect() throws Throwable {
		log.info("testFlatMapFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSortedCount() throws Throwable {
		log.info("testFlatMapFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterSortedForEach() throws Throwable {
		log.info("testFlatMapFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilterSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFilterCollect() throws Throwable {
		log.info("testFlatMapFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFilterCount() throws Throwable {
		log.info("testFlatMapFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFilterForEach() throws Throwable {
		log.info("testFlatMapFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFlatMapCollect() throws Throwable {
		log.info("testFlatMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFlatMapCount() throws Throwable {
		log.info("testFlatMapFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapFlatMapForEach() throws Throwable {
		log.info("testFlatMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapCollect() throws Throwable {
		log.info("testFlatMapFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapCount() throws Throwable {
		log.info("testFlatMapFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapForEach() throws Throwable {
		log.info("testFlatMapFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairCollect() throws Throwable {
		log.info("testFlatMapFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairCount() throws Throwable {
		log.info("testFlatMapFlatMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairForEach() throws Throwable {
		log.info("testFlatMapFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapPeekCollect() throws Throwable {
		log.info("testFlatMapFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapPeekCount() throws Throwable {
		log.info("testFlatMapFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapPeekForEach() throws Throwable {
		log.info("testFlatMapFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSampleCollect() throws Throwable {
		log.info("testFlatMapFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSampleCount() throws Throwable {
		log.info("testFlatMapFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSampleForEach() throws Throwable {
		log.info("testFlatMapFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSortedCollect() throws Throwable {
		log.info("testFlatMapFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSortedCount() throws Throwable {
		log.info("testFlatMapFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapSortedForEach() throws Throwable {
		log.info("testFlatMapFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFilterCollect() throws Throwable {
		log.info("testFlatMapMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFilterCount() throws Throwable {
		log.info("testFlatMapMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapMapFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFilterForEach() throws Throwable {
		log.info("testFlatMapMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFlatMapCollect() throws Throwable {
		log.info("testFlatMapMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFlatMapCount() throws Throwable {
		log.info("testFlatMapMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapFlatMapForEach() throws Throwable {
		log.info("testFlatMapMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapCollect() throws Throwable {
		log.info("testFlatMapMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapCount() throws Throwable {
		log.info("testFlatMapMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapForEach() throws Throwable {
		log.info("testFlatMapMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairCollect() throws Throwable {
		log.info("testFlatMapMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairCount() throws Throwable {
		log.info("testFlatMapMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairForEach() throws Throwable {
		log.info("testFlatMapMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPeekCollect() throws Throwable {
		log.info("testFlatMapMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPeekCount() throws Throwable {
		log.info("testFlatMapMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPeekForEach() throws Throwable {
		log.info("testFlatMapMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSampleCollect() throws Throwable {
		log.info("testFlatMapMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSampleCount() throws Throwable {
		log.info("testFlatMapMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSampleForEach() throws Throwable {
		log.info("testFlatMapMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSortedCollect() throws Throwable {
		log.info("testFlatMapMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSortedCount() throws Throwable {
		log.info("testFlatMapMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapSortedForEach() throws Throwable {
		log.info("testFlatMapMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFilterCollect() throws Throwable {
		log.info("testFlatMapMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFilterCount() throws Throwable {
		log.info("testFlatMapMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFilterForEach() throws Throwable {
		log.info("testFlatMapMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFlatMapCollect() throws Throwable {
		log.info("testFlatMapMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFlatMapCount() throws Throwable {
		log.info("testFlatMapMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairFlatMapForEach() throws Throwable {
		log.info("testFlatMapMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapCollect() throws Throwable {
		log.info("testFlatMapMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapCount() throws Throwable {
		log.info("testFlatMapMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapForEach() throws Throwable {
		log.info("testFlatMapMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairCollect() throws Throwable {
		log.info("testFlatMapMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairCount() throws Throwable {
		log.info("testFlatMapMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairForEach() throws Throwable {
		log.info("testFlatMapMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairPeekCollect() throws Throwable {
		log.info("testFlatMapMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairPeekCount() throws Throwable {
		log.info("testFlatMapMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairPeekForEach() throws Throwable {
		log.info("testFlatMapMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSampleCollect() throws Throwable {
		log.info("testFlatMapMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSampleCount() throws Throwable {
		log.info("testFlatMapMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSampleForEach() throws Throwable {
		log.info("testFlatMapMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSample After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSortedCollect() throws Throwable {
		log.info("testFlatMapMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSortedCount() throws Throwable {
		log.info("testFlatMapMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairSortedForEach() throws Throwable {
		log.info("testFlatMapMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMapPairSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFilterCollect() throws Throwable {
		log.info("testFlatMapPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapPeekFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFilterCount() throws Throwable {
		log.info("testFlatMapPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFilterForEach() throws Throwable {
		log.info("testFlatMapPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapPeekFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFlatMapCollect() throws Throwable {
		log.info("testFlatMapPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFlatMapCount() throws Throwable {
		log.info("testFlatMapPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekFlatMapForEach() throws Throwable {
		log.info("testFlatMapPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapCollect() throws Throwable {
		log.info("testFlatMapPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekMap After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapCount() throws Throwable {
		log.info("testFlatMapPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapForEach() throws Throwable {
		log.info("testFlatMapPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekMap After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairCollect() throws Throwable {
		log.info("testFlatMapPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairCount() throws Throwable {
		log.info("testFlatMapPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairForEach() throws Throwable {
		log.info("testFlatMapPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekPeekCollect() throws Throwable {
		log.info("testFlatMapPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekPeekCount() throws Throwable {
		log.info("testFlatMapPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekPeekForEach() throws Throwable {
		log.info("testFlatMapPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSampleCollect() throws Throwable {
		log.info("testFlatMapPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekSample After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSampleCount() throws Throwable {
		log.info("testFlatMapPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSampleForEach() throws Throwable {
		log.info("testFlatMapPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekSample After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSortedCollect() throws Throwable {
		log.info("testFlatMapPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSortedCount() throws Throwable {
		log.info("testFlatMapPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekSortedForEach() throws Throwable {
		log.info("testFlatMapPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeekSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFilterCollect() throws Throwable {
		log.info("testFlatMapSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapSampleFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFilterCount() throws Throwable {
		log.info("testFlatMapSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFilterForEach() throws Throwable {
		log.info("testFlatMapSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapSampleFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFlatMapCollect() throws Throwable {
		log.info("testFlatMapSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFlatMapCount() throws Throwable {
		log.info("testFlatMapSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleFlatMapForEach() throws Throwable {
		log.info("testFlatMapSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapCollect() throws Throwable {
		log.info("testFlatMapSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapCount() throws Throwable {
		log.info("testFlatMapSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapForEach() throws Throwable {
		log.info("testFlatMapSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairCollect() throws Throwable {
		log.info("testFlatMapSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairCount() throws Throwable {
		log.info("testFlatMapSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairForEach() throws Throwable {
		log.info("testFlatMapSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSamplePeekCollect() throws Throwable {
		log.info("testFlatMapSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSamplePeek After---------------------------------------");
	}

	@Test
	public void testFlatMapSamplePeekCount() throws Throwable {
		log.info("testFlatMapSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSamplePeekForEach() throws Throwable {
		log.info("testFlatMapSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSamplePeek After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSampleCollect() throws Throwable {
		log.info("testFlatMapSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSampleCount() throws Throwable {
		log.info("testFlatMapSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSampleForEach() throws Throwable {
		log.info("testFlatMapSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSortedCollect() throws Throwable {
		log.info("testFlatMapSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSortedCount() throws Throwable {
		log.info("testFlatMapSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleSortedForEach() throws Throwable {
		log.info("testFlatMapSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSampleSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFilterCollect() throws Throwable {
		log.info("testFlatMapSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFlatMapSortedFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFilterCount() throws Throwable {
		log.info("testFlatMapSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testFlatMapSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFilterForEach() throws Throwable {
		log.info("testFlatMapSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapSortedFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFlatMapCollect() throws Throwable {
		log.info("testFlatMapSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFlatMapCount() throws Throwable {
		log.info("testFlatMapSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedFlatMapForEach() throws Throwable {
		log.info("testFlatMapSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapCollect() throws Throwable {
		log.info("testFlatMapSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapCount() throws Throwable {
		log.info("testFlatMapSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapForEach() throws Throwable {
		log.info("testFlatMapSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedMap After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairCollect() throws Throwable {
		log.info("testFlatMapSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairCount() throws Throwable {
		log.info("testFlatMapSortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairForEach() throws Throwable {
		log.info("testFlatMapSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testFlatMapSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapSortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testFlatMapSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testFlatMapSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedPeekCollect() throws Throwable {
		log.info("testFlatMapSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedPeekCount() throws Throwable {
		log.info("testFlatMapSortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedPeekForEach() throws Throwable {
		log.info("testFlatMapSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSampleCollect() throws Throwable {
		log.info("testFlatMapSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSampleCount() throws Throwable {
		log.info("testFlatMapSortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSampleForEach() throws Throwable {
		log.info("testFlatMapSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSortedCollect() throws Throwable {
		log.info("testFlatMapSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSortedCount() throws Throwable {
		log.info("testFlatMapSortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testFlatMapSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedSortedForEach() throws Throwable {
		log.info("testFlatMapSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSortedSorted After---------------------------------------");
	}

	@Test
	public void testMapFilterFilterCollect() throws Throwable {
		log.info("testMapFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapFilterFilterCount() throws Throwable {
		log.info("testMapFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testMapFilterFilterForEach() throws Throwable {
		log.info("testMapFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapFilterFlatMapCollect() throws Throwable {
		log.info("testMapFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapFilterFlatMapCount() throws Throwable {
		log.info("testMapFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapFilterFlatMapForEach() throws Throwable {
		log.info("testMapFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapFilterMapCollect() throws Throwable {
		log.info("testMapFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterMap After---------------------------------------");
	}

	@Test
	public void testMapFilterMapCount() throws Throwable {
		log.info("testMapFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterMapCount After---------------------------------------");
	}

	@Test
	public void testMapFilterMapForEach() throws Throwable {
		log.info("testMapFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterMap After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairCollect() throws Throwable {
		log.info("testMapFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairCount() throws Throwable {
		log.info("testMapFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairForEach() throws Throwable {
		log.info("testMapFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testMapFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapFilterMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testMapFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testMapFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapFilterPeekCollect() throws Throwable {
		log.info("testMapFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapFilterPeekCount() throws Throwable {
		log.info("testMapFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testMapFilterPeekForEach() throws Throwable {
		log.info("testMapFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapFilterSampleCollect() throws Throwable {
		log.info("testMapFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterSample After---------------------------------------");
	}

	@Test
	public void testMapFilterSampleCount() throws Throwable {
		log.info("testMapFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testMapFilterSampleForEach() throws Throwable {
		log.info("testMapFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterSample After---------------------------------------");
	}

	@Test
	public void testMapFilterSortedCollect() throws Throwable {
		log.info("testMapFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapFilterSortedCount() throws Throwable {
		log.info("testMapFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testMapFilterSortedForEach() throws Throwable {
		log.info("testMapFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).sorted(new SortedComparator<String[]>() {
			public int compare(String[] value1, String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFilterCollect() throws Throwable {
		log.info("testMapFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFilterCount() throws Throwable {
		log.info("testMapFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFilterForEach() throws Throwable {
		log.info("testMapFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFlatMapCollect() throws Throwable {
		log.info("testMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFlatMapCount() throws Throwable {
		log.info("testMapFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapFlatMapForEach() throws Throwable {
		log.info("testMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapCollect() throws Throwable {
		log.info("testMapFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapCount() throws Throwable {
		log.info("testMapFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapForEach() throws Throwable {
		log.info("testMapFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairCollect() throws Throwable {
		log.info("testMapFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairCount() throws Throwable {
		log.info("testMapFlatMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairForEach() throws Throwable {
		log.info("testMapFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				log.info(tuple2.v2);
				if (tuple2.v2 != null) {
					sum += ((List) tuple2.v2).size();
				}
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(289, sum);

		log.info("testMapFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(289, sum);

		log.info("testMapFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {

				return (Tuple2<String, String>) Tuple.tuple(value, value);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(289, sum);

		log.info("testMapFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapFlatMapPeekCollect() throws Throwable {
		log.info("testMapFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapFlatMapPeekCount() throws Throwable {
		log.info("testMapFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapPeekForEach() throws Throwable {
		log.info("testMapFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSampleCollect() throws Throwable {
		log.info("testMapFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSampleCount() throws Throwable {
		log.info("testMapFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSampleForEach() throws Throwable {
		log.info("testMapFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSortedCollect() throws Throwable {
		log.info("testMapFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSortedCount() throws Throwable {
		log.info("testMapFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapSortedForEach() throws Throwable {
		log.info("testMapFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapMapFilterCollect() throws Throwable {
		log.info("testMapMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split("-")[1]) && !"ArrDelay".equals(value.split("-")[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapMapFilter After---------------------------------------");
	}

	@Test
	public void testMapMapFilterCount() throws Throwable {
		log.info("testMapMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split("-")[1]) && !"ArrDelay".equals(value.split("-")[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapMapFilterForEach() throws Throwable {
		log.info("testMapMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split("-")[1]) && !"ArrDelay".equals(value.split("-")[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapMapFilter After---------------------------------------");
	}

	@Test
	public void testMapMapFlatMapCollect() throws Throwable {
		log.info("testMapMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapFlatMapCount() throws Throwable {
		log.info("testMapMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapFlatMapForEach() throws Throwable {
		log.info("testMapMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapMapCollect() throws Throwable {
		log.info("testMapMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapMap After---------------------------------------");
	}

	@Test
	public void testMapMapMapCount() throws Throwable {
		log.info("testMapMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapMapForEach() throws Throwable {
		log.info("testMapMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapMap After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairCollect() throws Throwable {
		log.info("testMapMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairCount() throws Throwable {
		log.info("testMapMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairForEach() throws Throwable {
		log.info("testMapMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split("-")[0], value.split("-")[1]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPeekCollect() throws Throwable {
		log.info("testMapMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPeekCount() throws Throwable {
		log.info("testMapMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapMapPeekForEach() throws Throwable {
		log.info("testMapMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPeek After---------------------------------------");
	}

	@Test
	public void testMapMapSampleCollect() throws Throwable {
		log.info("testMapMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapSample After---------------------------------------");
	}

	@Test
	public void testMapMapSampleCount() throws Throwable {
		log.info("testMapMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapMapSampleForEach() throws Throwable {
		log.info("testMapMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapSample After---------------------------------------");
	}

	@Test
	public void testMapMapSortedCollect() throws Throwable {
		log.info("testMapMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapSorted After---------------------------------------");
	}

	@Test
	public void testMapMapSortedCount() throws Throwable {
		log.info("testMapMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapMapSortedForEach() throws Throwable {
		log.info("testMapMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapSorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairFilterCollect() throws Throwable {
		log.info("testMapMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairFilterCount() throws Throwable {
		log.info("testMapMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairFilterForEach() throws Throwable {
		log.info("testMapMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairFlatMapCollect() throws Throwable {
		log.info("testMapMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairFlatMapCount() throws Throwable {
		log.info("testMapMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairFlatMapForEach() throws Throwable {
		log.info("testMapMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testMapMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testMapMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapCollect() throws Throwable {
		log.info("testMapMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapCount() throws Throwable {
		log.info("testMapMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapForEach() throws Throwable {
		log.info("testMapMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairCollect() throws Throwable {
		log.info("testMapMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairCount() throws Throwable {
		log.info("testMapMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairForEach() throws Throwable {
		log.info("testMapMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairPeekCollect() throws Throwable {
		log.info("testMapMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairPeekCount() throws Throwable {
		log.info("testMapMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairPeekForEach() throws Throwable {
		log.info("testMapMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).filter(new PredicateSerializable<Tuple2>() {
			public boolean test(Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).mapToPair(new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
			public Tuple2 apply(Tuple2 value) {
				return (Tuple2<String, String>) value;
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testMapMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testMapMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testMapMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testMapMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairSampleCollect() throws Throwable {
		log.info("testMapMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapMapPairSampleCount() throws Throwable {
		log.info("testMapMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairSampleForEach() throws Throwable {
		log.info("testMapMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapMapPairSortedCollect() throws Throwable {
		log.info("testMapMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapMapPairSortedCount() throws Throwable {
		log.info("testMapMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairSortedForEach() throws Throwable {
		log.info("testMapMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).sorted(new SortedComparator<Tuple2>() {
			public int compare(Tuple2 value1, Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPeekFilterCollect() throws Throwable {
		log.info("testMapPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPeekFilterCount() throws Throwable {
		log.info("testMapPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testMapPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPeekFilterForEach() throws Throwable {
		log.info("testMapPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String[]>() {
			public boolean test(String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPeekFlatMapCollect() throws Throwable {
		log.info("testMapPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPeekFlatMapCount() throws Throwable {
		log.info("testMapPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPeekFlatMapForEach() throws Throwable {
		log.info("testMapPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String[], String>() {
			public Stream<String> apply(String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPeekMapCollect() throws Throwable {
		log.info("testMapPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPeekMapCount() throws Throwable {
		log.info("testMapPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekMapCount After---------------------------------------");
	}

	@Test
	public void testMapPeekMapForEach() throws Throwable {
		log.info("testMapPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).map(new MapFunction<String[], String>() {
			public String apply(String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairCollect() throws Throwable {
		log.info("testMapPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairCount() throws Throwable {
		log.info("testMapPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairForEach() throws Throwable {
		log.info("testMapPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).mapToPair(new MapToPairFunction<String[], Tuple2<String, String>>() {
			public Tuple2 apply(String[] value) {
				return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPeekPeekCollect() throws Throwable {
		log.info("testMapPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPeekPeekCount() throws Throwable {
		log.info("testMapPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPeekPeekForEach() throws Throwable {
		log.info("testMapPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPeekSampleCollect() throws Throwable {
		log.info("testMapPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPeekSampleCount() throws Throwable {
		log.info("testMapPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPeekSampleCount After---------------------------------------");
	}

}
