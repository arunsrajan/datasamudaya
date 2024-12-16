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

@SuppressWarnings({ "unchecked", "serial", "rawtypes" })
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineDepth2Test extends StreamPipelineBaseTestCommon {
	boolean toexecute = true;
	Logger log = LogManager.getLogger(StreamPipelineDepth2Test.class);
	int sum;

	@Test
	public void testFilterFilterCollect() throws Throwable {
		log.info("testFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterFilter After---------------------------------------");
	}

	@Test
	public void testFilterFilterCount() throws Throwable {
		log.info("testFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testFilterFilterForEach() throws Throwable {
		log.info("testFilterFilter Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFilter After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapCollect() throws Throwable {
		log.info("testFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapCount() throws Throwable {
		log.info("testFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFilterFlatMapForEach() throws Throwable {
		log.info("testFilterFlatMap Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testFilterMapCollect() throws Throwable {
		log.info("testFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterMap After---------------------------------------");
	}

	@Test
	public void testFilterMapCount() throws Throwable {
		log.info("testFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterMapCount After---------------------------------------");
	}

	@Test
	public void testFilterMapForEach() throws Throwable {
		log.info("testFilterMap Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterMap After---------------------------------------");
	}

	@Test
	public void testFilterMapPairCollect() throws Throwable {
		log.info("testFilterMapPair Before---------------------------------------");
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

		log.info("testFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairCount() throws Throwable {
		log.info("testFilterMapPairCount Before---------------------------------------");
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

		log.info("testFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairForEach() throws Throwable {
		log.info("testFilterMapPair Before---------------------------------------");
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

		log.info("testFilterMapPair After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFilterMapPairGroupByKey Before---------------------------------------");
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

		log.info("testFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFilterMapPairGroupByKey Before---------------------------------------");
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

		log.info("testFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(1, sum);

		log.info("testFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testFilterMapPairReduceByKeyCount Before---------------------------------------");
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

		log.info("testFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFilterMapPairReduceByKey Before---------------------------------------");
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

		log.info("testFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFilterPeekCollect() throws Throwable {
		log.info("testFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterPeek After---------------------------------------");
	}

	@Test
	public void testFilterPeekCount() throws Throwable {
		log.info("testFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testFilterPeekForEach() throws Throwable {
		log.info("testFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterPeek After---------------------------------------");
	}

	@Test
	public void testFilterSampleCollect() throws Throwable {
		log.info("testFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterSample After---------------------------------------");
	}

	@Test
	public void testFilterSampleCount() throws Throwable {
		log.info("testFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testFilterSampleForEach() throws Throwable {
		log.info("testFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSample After---------------------------------------");
	}

	@Test
	public void testFilterSortedCollect() throws Throwable {
		log.info("testFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterSorted After---------------------------------------");
	}

	@Test
	public void testFilterSortedCount() throws Throwable {
		log.info("testFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.filter(new PredicateSerializable<String>() {
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

		log.info("testFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testFilterSortedForEach() throws Throwable {
		log.info("testFilterSorted Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFilterSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterCollect() throws Throwable {
		log.info("testFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterCount() throws Throwable {
		log.info("testFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFilterForEach() throws Throwable {
		log.info("testFlatMapFilter Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapCollect() throws Throwable {
		log.info("testFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapCount() throws Throwable {
		log.info("testFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapFlatMapForEach() throws Throwable {
		log.info("testFlatMapFlatMap Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapCollect() throws Throwable {
		log.info("testFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapCount() throws Throwable {
		log.info("testFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapForEach() throws Throwable {
		log.info("testFlatMapMap Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapMap After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairCollect() throws Throwable {
		log.info("testFlatMapMapPair Before---------------------------------------");
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

		log.info("testFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairCount() throws Throwable {
		log.info("testFlatMapMapPairCount Before---------------------------------------");
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

		log.info("testFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairForEach() throws Throwable {
		log.info("testFlatMapMapPair Before---------------------------------------");
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

		log.info("testFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairGroupByKey Before---------------------------------------");
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

		log.info("testFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairGroupByKey Before---------------------------------------");
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

		log.info("testFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testFlatMapMapPairReduceByKey Before---------------------------------------");
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

		log.info("testFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testFlatMapMapPairReduceByKeyCount Before---------------------------------------");
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

		log.info("testFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testFlatMapMapPairReduceByKey Before---------------------------------------");
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

		log.info("testFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekCollect() throws Throwable {
		log.info("testFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekCount() throws Throwable {
		log.info("testFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testFlatMapPeekForEach() throws Throwable {
		log.info("testFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleCollect() throws Throwable {
		log.info("testFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleCount() throws Throwable {
		log.info("testFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSampleForEach() throws Throwable {
		log.info("testFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSample After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedCollect() throws Throwable {
		log.info("testFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedCount() throws Throwable {
		log.info("testFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testFlatMapSortedForEach() throws Throwable {
		log.info("testFlatMapSorted Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapFilterCollect() throws Throwable {
		log.info("testMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapFilter After---------------------------------------");
	}

	@Test
	public void testMapFilterCount() throws Throwable {
		log.info("testMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapFilterForEach() throws Throwable {
		log.info("testMapFilter Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapFilter After---------------------------------------");
	}

	@Test
	public void testMapFlatMapCollect() throws Throwable {
		log.info("testMapFlatMap Before---------------------------------------");
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

		log.info("testMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapFlatMapCount() throws Throwable {
		log.info("testMapFlatMapCount Before---------------------------------------");
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

		log.info("testMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapFlatMapForEach() throws Throwable {
		log.info("testMapFlatMap Before---------------------------------------");
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

		log.info("testMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapMapCollect() throws Throwable {
		log.info("testMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapMap After---------------------------------------");
	}

	@Test
	public void testMapMapCount() throws Throwable {
		log.info("testMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapMapForEach() throws Throwable {
		log.info("testMapMap Before---------------------------------------");
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
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapMap After---------------------------------------");
	}

	@Test
	public void testMapMapPairCollect() throws Throwable {
		log.info("testMapMapPair Before---------------------------------------");
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

		log.info("testMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairCount() throws Throwable {
		log.info("testMapMapPairCount Before---------------------------------------");
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

		log.info("testMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairForEach() throws Throwable {
		log.info("testMapMapPair Before---------------------------------------");
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

		log.info("testMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapMapPairGroupByKey Before---------------------------------------");
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

		log.info("testMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapMapPairGroupByKey Before---------------------------------------");
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

		log.info("testMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapMapPairReduceByKey Before---------------------------------------");
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

		log.info("testMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapMapPairReduceByKeyCount Before---------------------------------------");
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

		log.info("testMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapMapPairReduceByKey Before---------------------------------------");
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

		log.info("testMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPeekCollect() throws Throwable {
		log.info("testMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPeekCount() throws Throwable {
		log.info("testMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPeekForEach() throws Throwable {
		log.info("testMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeek After---------------------------------------");
	}

	@Test
	public void testMapSampleCollect() throws Throwable {
		log.info("testMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapSample After---------------------------------------");
	}

	@Test
	public void testMapSampleCount() throws Throwable {
		log.info("testMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapSampleForEach() throws Throwable {
		log.info("testMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSample After---------------------------------------");
	}

	@Test
	public void testMapSortedCollect() throws Throwable {
		log.info("testMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapSorted After---------------------------------------");
	}

	@Test
	public void testMapSortedCount() throws Throwable {
		log.info("testMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapSortedForEach() throws Throwable {
		log.info("testMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new MapFunction<String, String[]>() {
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

		log.info("testMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairFilterCollect() throws Throwable {
		log.info("testMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFilterCount() throws Throwable {
		log.info("testMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterForEach() throws Throwable {
		log.info("testMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapCollect() throws Throwable {
		log.info("testMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapCount() throws Throwable {
		log.info("testMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapForEach() throws Throwable {
		log.info("testMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapCollect() throws Throwable {
		log.info("testMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapCount() throws Throwable {
		log.info("testMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapForEach() throws Throwable {
		log.info("testMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairCollect() throws Throwable {
		log.info("testMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairCount() throws Throwable {
		log.info("testMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairForEach() throws Throwable {
		log.info("testMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairPeekCollect() throws Throwable {
		log.info("testMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairPeekCount() throws Throwable {
		log.info("testMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekForEach() throws Throwable {
		log.info("testMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairSampleCollect() throws Throwable {
		log.info("testMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairSampleCount() throws Throwable {
		log.info("testMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleForEach() throws Throwable {
		log.info("testMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairSortedCollect() throws Throwable {
		log.info("testMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairSortedCount() throws Throwable {
		log.info("testMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedForEach() throws Throwable {
		log.info("testMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testMapPairSorted After---------------------------------------");
	}

	@Test
	public void testPeekFilterCollect() throws Throwable {
		log.info("testPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new PredicateSerializable<String>() {
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

		log.info("testPeekFilter After---------------------------------------");
	}

	@Test
	public void testPeekFilterCount() throws Throwable {
		log.info("testPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.filter(new PredicateSerializable<String>() {
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

		log.info("testPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testPeekFilterForEach() throws Throwable {
		log.info("testPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testPeekFilter After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapCollect() throws Throwable {
		log.info("testPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapCount() throws Throwable {
		log.info("testPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testPeekFlatMapForEach() throws Throwable {
		log.info("testPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testPeekMapCollect() throws Throwable {
		log.info("testPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new MapFunction<String, String[]>() {
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

		log.info("testPeekMap After---------------------------------------");
	}

	@Test
	public void testPeekMapCount() throws Throwable {
		log.info("testPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.map(new MapFunction<String, String[]>() {
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

		log.info("testPeekMapCount After---------------------------------------");
	}

	@Test
	public void testPeekMapForEach() throws Throwable {
		log.info("testPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekMap After---------------------------------------");
	}

	@Test
	public void testPeekMapPairCollect() throws Throwable {
		log.info("testPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testPeekMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairCount() throws Throwable {
		log.info("testPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairForEach() throws Throwable {
		log.info("testPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPair After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val))
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testPeekPeekCollect() throws Throwable {
		log.info("testPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekPeek After---------------------------------------");
	}

	@Test
	public void testPeekPeekCount() throws Throwable {
		log.info("testPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testPeekPeekForEach() throws Throwable {
		log.info("testPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekPeek After---------------------------------------");
	}

	@Test
	public void testPeekSampleCollect() throws Throwable {
		log.info("testPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).collect(toexecute,
				null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testPeekSample After---------------------------------------");
	}

	@Test
	public void testPeekSampleCount() throws Throwable {
		log.info("testPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testPeekSampleForEach() throws Throwable {
		log.info("testPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSample After---------------------------------------");
	}

	@Test
	public void testPeekSortedCollect() throws Throwable {
		log.info("testPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new SortedComparator<String>() {
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

		log.info("testPeekSorted After---------------------------------------");
	}

	@Test
	public void testPeekSortedCount() throws Throwable {
		log.info("testPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.peek(val -> System.out.println(val))
				.sorted(new SortedComparator<String>() {
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

		log.info("testPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testPeekSortedForEach() throws Throwable {
		log.info("testPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testPeekSorted After---------------------------------------");
	}

	@Test
	public void testSampleFilterCollect() throws Throwable {
		log.info("testSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).filter(new PredicateSerializable<String>() {
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

		log.info("testSampleFilter After---------------------------------------");
	}

	@Test
	public void testSampleFilterCount() throws Throwable {
		log.info("testSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).filter(new PredicateSerializable<String>() {
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

		log.info("testSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleFilterForEach() throws Throwable {
		log.info("testSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleFilter After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapCollect() throws Throwable {
		log.info("testSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapCount() throws Throwable {
		log.info("testSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).flatMap(new FlatMapFunction<String, String>() {
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

		log.info("testSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleFlatMapForEach() throws Throwable {
		log.info("testSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapCollect() throws Throwable {
		log.info("testSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).map(new MapFunction<String, String[]>() {
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

		log.info("testSampleMap After---------------------------------------");
	}

	@Test
	public void testSampleMapCount() throws Throwable {
		log.info("testSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).map(new MapFunction<String, String[]>() {
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

		log.info("testSampleMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapForEach() throws Throwable {
		log.info("testSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairCollect() throws Throwable {
		log.info("testSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairCount() throws Throwable {
		log.info("testSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairForEach() throws Throwable {
		log.info("testSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361)
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(new MapToPairFunction<String, Tuple2<String, String>>() {
			public Tuple2 apply(String value) {
				return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
			}
		}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSamplePeekCollect() throws Throwable {
		log.info("testSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).collect(toexecute,
				null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSamplePeek After---------------------------------------");
	}

	@Test
	public void testSamplePeekCount() throws Throwable {
		log.info("testSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekForEach() throws Throwable {
		log.info("testSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeek After---------------------------------------");
	}

	@Test
	public void testSampleSampleCollect() throws Throwable {
		log.info("testSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleSample After---------------------------------------");
	}

	@Test
	public void testSampleSampleCount() throws Throwable {
		log.info("testSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleForEach() throws Throwable {
		log.info("testSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSample After---------------------------------------");
	}

	@Test
	public void testSampleSortedCollect() throws Throwable {
		log.info("testSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSorted After---------------------------------------");
	}

	@Test
	public void testSampleSortedCount() throws Throwable {
		log.info("testSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedForEach() throws Throwable {
		log.info("testSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSorted After---------------------------------------");
	}

	@Test
	public void testSortedFilterCollect() throws Throwable {
		log.info("testSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFilter After---------------------------------------");
	}

	@Test
	public void testSortedFilterCount() throws Throwable {
		log.info("testSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterForEach() throws Throwable {
		log.info("testSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFilter After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapCollect() throws Throwable {
		log.info("testSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapCount() throws Throwable {
		log.info("testSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapForEach() throws Throwable {
		log.info("testSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapCollect() throws Throwable {
		log.info("testSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMap After---------------------------------------");
	}

	@Test
	public void testSortedMapCount() throws Throwable {
		log.info("testSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapForEach() throws Throwable {
		log.info("testSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairCollect() throws Throwable {
		log.info("testSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairCount() throws Throwable {
		log.info("testSortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairForEach() throws Throwable {
		log.info("testSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedPeekCollect() throws Throwable {
		log.info("testSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedPeek After---------------------------------------");
	}

	@Test
	public void testSortedPeekCount() throws Throwable {
		log.info("testSortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekForEach() throws Throwable {
		log.info("testSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeek After---------------------------------------");
	}

	@Test
	public void testSortedSampleCollect() throws Throwable {
		log.info("testSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedSample After---------------------------------------");
	}

	@Test
	public void testSortedSampleCount() throws Throwable {
		log.info("testSortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleForEach() throws Throwable {
		log.info("testSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSample After---------------------------------------");
	}

	@Test
	public void testSortedSortedCollect() throws Throwable {
		log.info("testSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedSorted After---------------------------------------");
	}

	@Test
	public void testSortedSortedCount() throws Throwable {
		log.info("testSortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedForEach() throws Throwable {
		log.info("testSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
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

		log.info("testSortedSorted After---------------------------------------");
	}

}
