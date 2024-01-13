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

import org.apache.log4j.Logger;
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

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IgnitePipelineDepth34Test extends StreamPipelineIgniteBase {

	boolean toexecute = true;
	int sum;
	static Logger log = Logger.getLogger(IgnitePipelineDepth34Test.class);
	@Test
	public void testSampleMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new PredicateSerializable<Tuple2>() {
					public boolean test(Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testSampleMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSampleMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testSampleMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSampleMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testSampleMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
					public int compare(Tuple2 value1, Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSampleCollect() throws Throwable {
		log.info("testSampleMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairSample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSampleCount() throws Throwable {
		log.info("testSampleMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSampleForEach() throws Throwable {
		log.info("testSampleMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleMapPairSample After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSortedCollect() throws Throwable {
		log.info("testSampleMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairSorted After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSortedCount() throws Throwable {
		log.info("testSampleMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleMapPairSortedForEach() throws Throwable {
		log.info("testSampleMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleMapPairSorted After---------------------------------------");
	}

	@Test
	public void testSamplePeekFilterCollect() throws Throwable {
		log.info("testSamplePeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekFilter After---------------------------------------");
	}

	@Test
	public void testSamplePeekFilterCount() throws Throwable {
		log.info("testSamplePeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekFilterCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekFilterForEach() throws Throwable {
		log.info("testSamplePeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val))
				.filter(new PredicateSerializable<String>() {
					public boolean test(String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSamplePeekFilter After---------------------------------------");
	}

	@Test
	public void testSamplePeekFlatMapCollect() throws Throwable {
		log.info("testSamplePeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekFlatMap After---------------------------------------");
	}

	@Test
	public void testSamplePeekFlatMapCount() throws Throwable {
		log.info("testSamplePeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekFlatMapForEach() throws Throwable {
		log.info("testSamplePeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val))
				.flatMap(new FlatMapFunction<String, String>() {
					public Stream<String> apply(String value) {
						return Arrays.asList(value).stream();
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekFlatMap After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapCollect() throws Throwable {
		log.info("testSamplePeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekMap After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapCount() throws Throwable {
		log.info("testSamplePeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekMapCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapForEach() throws Throwable {
		log.info("testSamplePeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val))
				.map(new MapFunction<String, String[]>() {
					public String[] apply(String value) {
						return value.split(",");
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekMap After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairCollect() throws Throwable {
		log.info("testSamplePeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSamplePeekMapPair After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairCount() throws Throwable {
		log.info("testSamplePeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSamplePeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairForEach() throws Throwable {
		log.info("testSamplePeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekMapPair After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSamplePeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSamplePeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSamplePeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSamplePeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSamplePeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testSamplePeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSamplePeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSamplePeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSamplePeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSamplePeekPeekCollect() throws Throwable {
		log.info("testSamplePeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSamplePeekPeek After---------------------------------------");
	}

	@Test
	public void testSamplePeekPeekCount() throws Throwable {
		log.info("testSamplePeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSamplePeekPeekCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekPeekForEach() throws Throwable {
		log.info("testSamplePeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekPeek After---------------------------------------");
	}

	@Test
	public void testSamplePeekSampleCollect() throws Throwable {
		log.info("testSamplePeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).sample(46361).collect(toexecute,
				null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSamplePeekSample After---------------------------------------");
	}

	@Test
	public void testSamplePeekSampleCount() throws Throwable {
		log.info("testSamplePeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSamplePeekSampleCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekSampleForEach() throws Throwable {
		log.info("testSamplePeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekSample After---------------------------------------");
	}

	@Test
	public void testSamplePeekSortedCollect() throws Throwable {
		log.info("testSamplePeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekSorted After---------------------------------------");
	}

	@Test
	public void testSamplePeekSortedCount() throws Throwable {
		log.info("testSamplePeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).peek(val -> System.out.println(val))
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

		log.info("testSamplePeekSortedCount After---------------------------------------");
	}

	@Test
	public void testSamplePeekSortedForEach() throws Throwable {
		log.info("testSamplePeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).peek(val -> System.out.println(val))
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSamplePeekSorted After---------------------------------------");
	}

	@Test
	public void testSampleSampleFilterCollect() throws Throwable {
		log.info("testSampleSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleFilter After---------------------------------------");
	}

	@Test
	public void testSampleSampleFilterCount() throws Throwable {
		log.info("testSampleSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleFilterForEach() throws Throwable {
		log.info("testSampleSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361)
				.filter(new PredicateSerializable<String>() {
					public boolean test(String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSampleSampleFilter After---------------------------------------");
	}

	@Test
	public void testSampleSampleFlatMapCollect() throws Throwable {
		log.info("testSampleSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleSampleFlatMapCount() throws Throwable {
		log.info("testSampleSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleFlatMapForEach() throws Throwable {
		log.info("testSampleSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361)
				.flatMap(new FlatMapFunction<String, String>() {
					public Stream<String> apply(String value) {
						return Arrays.asList(value).stream();
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapCollect() throws Throwable {
		log.info("testSampleSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleMap After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapCount() throws Throwable {
		log.info("testSampleSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleMapCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapForEach() throws Throwable {
		log.info("testSampleSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361)
				.map(new MapFunction<String, String[]>() {
					public String[] apply(String value) {
						return value.split(",");
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleMap After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairCollect() throws Throwable {
		log.info("testSampleSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairCount() throws Throwable {
		log.info("testSampleSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairForEach() throws Throwable {
		log.info("testSampleSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSampleSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleSamplePeekCollect() throws Throwable {
		log.info("testSampleSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361).peek(val -> System.out.println(val)).collect(toexecute,
				null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleSamplePeek After---------------------------------------");
	}

	@Test
	public void testSampleSamplePeekCount() throws Throwable {
		log.info("testSampleSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testSampleSamplePeekForEach() throws Throwable {
		log.info("testSampleSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSamplePeek After---------------------------------------");
	}

	@Test
	public void testSampleSampleSampleCollect() throws Throwable {
		log.info("testSampleSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleSampleSample After---------------------------------------");
	}

	@Test
	public void testSampleSampleSampleCount() throws Throwable {
		log.info("testSampleSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleSampleForEach() throws Throwable {
		log.info("testSampleSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleSample After---------------------------------------");
	}

	@Test
	public void testSampleSampleSortedCollect() throws Throwable {
		log.info("testSampleSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleSorted After---------------------------------------");
	}

	@Test
	public void testSampleSampleSortedCount() throws Throwable {
		log.info("testSampleSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361).sample(46361)
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

		log.info("testSampleSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleSampleSortedForEach() throws Throwable {
		log.info("testSampleSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSampleSorted After---------------------------------------");
	}

	@Test
	public void testSampleSortedFilterCollect() throws Throwable {
		log.info("testSampleSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFilter After---------------------------------------");
	}

	@Test
	public void testSampleSortedFilterCount() throws Throwable {
		log.info("testSampleSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedFilterForEach() throws Throwable {
		log.info("testSampleSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFilter After---------------------------------------");
	}

	@Test
	public void testSampleSortedFlatMapCollect() throws Throwable {
		log.info("testSampleSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleSortedFlatMapCount() throws Throwable {
		log.info("testSampleSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedFlatMapForEach() throws Throwable {
		log.info("testSampleSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapCollect() throws Throwable {
		log.info("testSampleSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedMap After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapCount() throws Throwable {
		log.info("testSampleSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedMapCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapForEach() throws Throwable {
		log.info("testSampleSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedMap After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairCollect() throws Throwable {
		log.info("testSampleSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSampleSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairCount() throws Throwable {
		log.info("testSampleSortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairForEach() throws Throwable {
		log.info("testSampleSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSampleSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSampleSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSampleSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSampleSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSampleSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testSampleSortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSampleSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSampleSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSampleSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSampleSortedPeekCollect() throws Throwable {
		log.info("testSampleSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedPeek After---------------------------------------");
	}

	@Test
	public void testSampleSortedPeekCount() throws Throwable {
		log.info("testSampleSortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedPeekForEach() throws Throwable {
		log.info("testSampleSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSortedPeek After---------------------------------------");
	}

	@Test
	public void testSampleSortedSampleCollect() throws Throwable {
		log.info("testSampleSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedSample After---------------------------------------");
	}

	@Test
	public void testSampleSortedSampleCount() throws Throwable {
		log.info("testSampleSortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedSampleForEach() throws Throwable {
		log.info("testSampleSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSampleSortedSample After---------------------------------------");
	}

	@Test
	public void testSampleSortedSortedCollect() throws Throwable {
		log.info("testSampleSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedSorted After---------------------------------------");
	}

	@Test
	public void testSampleSortedSortedCount() throws Throwable {
		log.info("testSampleSortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.sample(46361)
				.sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testSampleSortedSortedForEach() throws Throwable {
		log.info("testSampleSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sample(46361).sorted(new SortedComparator<String>() {
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

		log.info("testSampleSortedSorted After---------------------------------------");
	}

	@Test
	public void testSortedFilterFilterCollect() throws Throwable {
		log.info("testSortedFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterFilter After---------------------------------------");
	}

	@Test
	public void testSortedFilterFilterCount() throws Throwable {
		log.info("testSortedFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterFilterForEach() throws Throwable {
		log.info("testSortedFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterFilter After---------------------------------------");
	}

	@Test
	public void testSortedFilterFlatMapCollect() throws Throwable {
		log.info("testSortedFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedFilterFlatMapCount() throws Throwable {
		log.info("testSortedFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterFlatMapForEach() throws Throwable {
		log.info("testSortedFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapCollect() throws Throwable {
		log.info("testSortedFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterMap After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapCount() throws Throwable {
		log.info("testSortedFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterMapCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapForEach() throws Throwable {
		log.info("testSortedFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterMap After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairCollect() throws Throwable {
		log.info("testSortedFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedFilterMapPair After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairCount() throws Throwable {
		log.info("testSortedFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(45957, sum);

		log.info("testSortedFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairForEach() throws Throwable {
		log.info("testSortedFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSortedFilterMapPair After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(45957, sum);

		log.info("testSortedFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(45957, sum);

		log.info("testSortedFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedFilterMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(1, sum);

		log.info("testSortedFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(1, sum);

		log.info("testSortedFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedFilterPeekCollect() throws Throwable {
		log.info("testSortedFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterPeek After---------------------------------------");
	}

	@Test
	public void testSortedFilterPeekCount() throws Throwable {
		log.info("testSortedFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterPeekForEach() throws Throwable {
		log.info("testSortedFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSortedFilterPeek After---------------------------------------");
	}

	@Test
	public void testSortedFilterSampleCollect() throws Throwable {
		log.info("testSortedFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterSample After---------------------------------------");
	}

	@Test
	public void testSortedFilterSampleCount() throws Throwable {
		log.info("testSortedFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterSampleForEach() throws Throwable {
		log.info("testSortedFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSortedFilterSample After---------------------------------------");
	}

	@Test
	public void testSortedFilterSortedCollect() throws Throwable {
		log.info("testSortedFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterSorted After---------------------------------------");
	}

	@Test
	public void testSortedFilterSortedCount() throws Throwable {
		log.info("testSortedFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedFilterSortedForEach() throws Throwable {
		log.info("testSortedFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFilterSorted After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFilterCollect() throws Throwable {
		log.info("testSortedFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFilterCount() throws Throwable {
		log.info("testSortedFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFilterForEach() throws Throwable {
		log.info("testSortedFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFlatMapCollect() throws Throwable {
		log.info("testSortedFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFlatMapCount() throws Throwable {
		log.info("testSortedFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapFlatMapForEach() throws Throwable {
		log.info("testSortedFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapCollect() throws Throwable {
		log.info("testSortedFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapMap After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapCount() throws Throwable {
		log.info("testSortedFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapForEach() throws Throwable {
		log.info("testSortedFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapMap After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairCollect() throws Throwable {
		log.info("testSortedFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairCount() throws Throwable {
		log.info("testSortedFlatMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairForEach() throws Throwable {
		log.info("testSortedFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapPeekCollect() throws Throwable {
		log.info("testSortedFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapPeekCount() throws Throwable {
		log.info("testSortedFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapPeekForEach() throws Throwable {
		log.info("testSortedFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSampleCollect() throws Throwable {
		log.info("testSortedFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapSample After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSampleCount() throws Throwable {
		log.info("testSortedFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSampleForEach() throws Throwable {
		log.info("testSortedFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedFlatMapSample After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSortedCollect() throws Throwable {
		log.info("testSortedFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSortedCount() throws Throwable {
		log.info("testSortedFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedFlatMapSortedForEach() throws Throwable {
		log.info("testSortedFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testSortedMapFilterCollect() throws Throwable {
		log.info("testSortedMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapFilterCount() throws Throwable {
		log.info("testSortedMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedMapFilterForEach() throws Throwable {
		log.info("testSortedMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapFlatMapCollect() throws Throwable {
		log.info("testSortedMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapFlatMapCount() throws Throwable {
		log.info("testSortedMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapFlatMapForEach() throws Throwable {
		log.info("testSortedMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapMapCollect() throws Throwable {
		log.info("testSortedMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapMap After---------------------------------------");
	}

	@Test
	public void testSortedMapMapCount() throws Throwable {
		log.info("testSortedMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapMapForEach() throws Throwable {
		log.info("testSortedMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapMap After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairCollect() throws Throwable {
		log.info("testSortedMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
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

		log.info("testSortedMapMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairCount() throws Throwable {
		log.info("testSortedMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairForEach() throws Throwable {
		log.info("testSortedMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
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

		log.info("testSortedMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).mapToPair(
				new MapToPairFunction<String[], Tuple2<String, String>>() {
					public Tuple2 apply(String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPeekCollect() throws Throwable {
		log.info("testSortedMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPeekCount() throws Throwable {
		log.info("testSortedMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPeekForEach() throws Throwable {
		log.info("testSortedMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapSampleCollect() throws Throwable {
		log.info("testSortedMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapSample After---------------------------------------");
	}

	@Test
	public void testSortedMapSampleCount() throws Throwable {
		log.info("testSortedMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedMapSampleForEach() throws Throwable {
		log.info("testSortedMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapSample After---------------------------------------");
	}

	@Test
	public void testSortedMapSortedCollect() throws Throwable {
		log.info("testSortedMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapSorted After---------------------------------------");
	}

	@Test
	public void testSortedMapSortedCount() throws Throwable {
		log.info("testSortedMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedMapSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedMapSortedForEach() throws Throwable {
		log.info("testSortedMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedMapSorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFilterCollect() throws Throwable {
		log.info("testSortedMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFilterCount() throws Throwable {
		log.info("testSortedMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.filter(new PredicateSerializable<Tuple2>() {
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

		log.info("testSortedMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFilterForEach() throws Throwable {
		log.info("testSortedMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFlatMapCollect() throws Throwable {
		log.info("testSortedMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFlatMapCount() throws Throwable {
		log.info("testSortedMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairFlatMapForEach() throws Throwable {
		log.info("testSortedMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().filter(new PredicateSerializable<Tuple2>() {
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

		log.info("testSortedMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new PredicateSerializable<Tuple2>() {
					public boolean test(Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testSortedMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSortedMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testSortedMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapCollect() throws Throwable {
		log.info("testSortedMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapCount() throws Throwable {
		log.info("testSortedMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapForEach() throws Throwable {
		log.info("testSortedMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairCollect() throws Throwable {
		log.info("testSortedMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairCount() throws Throwable {
		log.info("testSortedMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.mapToPair(
						new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
							public Tuple2 apply(Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairForEach() throws Throwable {
		log.info("testSortedMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.mapToPair(
						new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
							public Tuple2 apply(Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.mapToPair(
						new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
							public Tuple2 apply(Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairPeekCollect() throws Throwable {
		log.info("testSortedMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairPeekCount() throws Throwable {
		log.info("testSortedMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairPeekForEach() throws Throwable {
		log.info("testSortedMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new PredicateSerializable<Tuple2>() {
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

		log.info("testSortedMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new PredicateSerializable<Tuple2>() {
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

		log.info("testSortedMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new PredicateSerializable<Tuple2>() {
					public boolean test(Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new MapToPairFunction<Tuple2, Tuple2<String, String>>() {
					public Tuple2 apply(Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testSortedMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSortedMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testSortedMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSortedMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testSortedMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new SortedComparator<Tuple2>() {
					public int compare(Tuple2 value1, Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSampleCollect() throws Throwable {
		log.info("testSortedMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairSample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSampleCount() throws Throwable {
		log.info("testSortedMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSampleForEach() throws Throwable {
		log.info("testSortedMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedMapPairSample After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSortedCollect() throws Throwable {
		log.info("testSortedMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairSorted After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSortedCount() throws Throwable {
		log.info("testSortedMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.sorted(new SortedComparator<Tuple2>() {
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

		log.info("testSortedMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedMapPairSortedForEach() throws Throwable {
		log.info("testSortedMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedMapPairSorted After---------------------------------------");
	}

	@Test
	public void testSortedPeekFilterCollect() throws Throwable {
		log.info("testSortedPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedPeekFilter After---------------------------------------");
	}

	@Test
	public void testSortedPeekFilterCount() throws Throwable {
		log.info("testSortedPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val))
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

		log.info("testSortedPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekFilterForEach() throws Throwable {
		log.info("testSortedPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSortedPeekFilter After---------------------------------------");
	}

	@Test
	public void testSortedPeekFlatMapCollect() throws Throwable {
		log.info("testSortedPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val))
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

		log.info("testSortedPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedPeekFlatMapCount() throws Throwable {
		log.info("testSortedPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val))
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

		log.info("testSortedPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekFlatMapForEach() throws Throwable {
		log.info("testSortedPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val))
				.flatMap(new FlatMapFunction<String, String>() {
					public Stream<String> apply(String value) {
						return Arrays.asList(value).stream();
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapCollect() throws Throwable {
		log.info("testSortedPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val))
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

		log.info("testSortedPeekMap After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapCount() throws Throwable {
		log.info("testSortedPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val))
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

		log.info("testSortedPeekMapCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapForEach() throws Throwable {
		log.info("testSortedPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val))
				.map(new MapFunction<String, String[]>() {
					public String[] apply(String value) {
						return value.split(",");
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekMap After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairCollect() throws Throwable {
		log.info("testSortedPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedPeekMapPair After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairCount() throws Throwable {
		log.info("testSortedPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairForEach() throws Throwable {
		log.info("testSortedPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekMapPair After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedPeekPeekCollect() throws Throwable {
		log.info("testSortedPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSortedPeekPeek After---------------------------------------");
	}

	@Test
	public void testSortedPeekPeekCount() throws Throwable {
		log.info("testSortedPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekPeekForEach() throws Throwable {
		log.info("testSortedPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekPeek After---------------------------------------");
	}

	@Test
	public void testSortedPeekSampleCollect() throws Throwable {
		log.info("testSortedPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSortedPeekSample After---------------------------------------");
	}

	@Test
	public void testSortedPeekSampleCount() throws Throwable {
		log.info("testSortedPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekSampleForEach() throws Throwable {
		log.info("testSortedPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekSample After---------------------------------------");
	}

	@Test
	public void testSortedPeekSortedCollect() throws Throwable {
		log.info("testSortedPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedPeekSorted After---------------------------------------");
	}

	@Test
	public void testSortedPeekSortedCount() throws Throwable {
		log.info("testSortedPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedPeekSortedForEach() throws Throwable {
		log.info("testSortedPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedPeekSorted After---------------------------------------");
	}

	@Test
	public void testSortedSampleFilterCollect() throws Throwable {
		log.info("testSortedSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSampleFilter After---------------------------------------");
	}

	@Test
	public void testSortedSampleFilterCount() throws Throwable {
		log.info("testSortedSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleFilterForEach() throws Throwable {
		log.info("testSortedSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).filter(new PredicateSerializable<String>() {
			public boolean test(String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testSortedSampleFilter After---------------------------------------");
	}

	@Test
	public void testSortedSampleFlatMapCollect() throws Throwable {
		log.info("testSortedSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedSampleFlatMapCount() throws Throwable {
		log.info("testSortedSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361)
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

		log.info("testSortedSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleFlatMapForEach() throws Throwable {
		log.info("testSortedSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).flatMap(new FlatMapFunction<String, String>() {
			public Stream<String> apply(String value) {
				return Arrays.asList(value).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapCollect() throws Throwable {
		log.info("testSortedSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSampleMap After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapCount() throws Throwable {
		log.info("testSortedSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSampleMapCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapForEach() throws Throwable {
		log.info("testSortedSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).map(new MapFunction<String, String[]>() {
			public String[] apply(String value) {
				return value.split(",");
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleMap After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairCollect() throws Throwable {
		log.info("testSortedSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairCount() throws Throwable {
		log.info("testSortedSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairForEach() throws Throwable {
		log.info("testSortedSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleMapPair After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedSamplePeekCollect() throws Throwable {
		log.info("testSortedSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSortedSamplePeek After---------------------------------------");
	}

	@Test
	public void testSortedSamplePeekCount() throws Throwable {
		log.info("testSortedSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testSortedSamplePeekForEach() throws Throwable {
		log.info("testSortedSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSamplePeek After---------------------------------------");
	}

	@Test
	public void testSortedSampleSampleCollect() throws Throwable {
		log.info("testSortedSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testSortedSampleSample After---------------------------------------");
	}

	@Test
	public void testSortedSampleSampleCount() throws Throwable {
		log.info("testSortedSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleSampleForEach() throws Throwable {
		log.info("testSortedSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleSample After---------------------------------------");
	}

	@Test
	public void testSortedSampleSortedCollect() throws Throwable {
		log.info("testSortedSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSampleSorted After---------------------------------------");
	}

	@Test
	public void testSortedSampleSortedCount() throws Throwable {
		log.info("testSortedSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedSampleSortedForEach() throws Throwable {
		log.info("testSortedSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSampleSorted After---------------------------------------");
	}

	@Test
	public void testSortedSortedFilterCollect() throws Throwable {
		log.info("testSortedSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedFilter After---------------------------------------");
	}

	@Test
	public void testSortedSortedFilterCount() throws Throwable {
		log.info("testSortedSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedFilterForEach() throws Throwable {
		log.info("testSortedSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedFilter After---------------------------------------");
	}

	@Test
	public void testSortedSortedFlatMapCollect() throws Throwable {
		log.info("testSortedSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedSortedFlatMapCount() throws Throwable {
		log.info("testSortedSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedFlatMapForEach() throws Throwable {
		log.info("testSortedSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapCollect() throws Throwable {
		log.info("testSortedSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedMap After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapCount() throws Throwable {
		log.info("testSortedSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedMapCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapForEach() throws Throwable {
		log.info("testSortedSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedMap After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairCollect() throws Throwable {
		log.info("testSortedSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairCount() throws Throwable {
		log.info("testSortedSortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairForEach() throws Throwable {
		log.info("testSortedSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSortedMapPair After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testSortedSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.groupByKey().collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testSortedSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testSortedSortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testSortedSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
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

		log.info("testSortedSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testSortedSortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testSortedSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testSortedSortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new MapToPairFunction<String, Tuple2<String, String>>() {
					public Tuple2 apply(String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testSortedSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testSortedSortedPeekCollect() throws Throwable {
		log.info("testSortedSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedPeek After---------------------------------------");
	}

	@Test
	public void testSortedSortedPeekCount() throws Throwable {
		log.info("testSortedSortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedPeekForEach() throws Throwable {
		log.info("testSortedSortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSortedPeek After---------------------------------------");
	}

	@Test
	public void testSortedSortedSampleCollect() throws Throwable {
		log.info("testSortedSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedSample After---------------------------------------");
	}

	@Test
	public void testSortedSortedSampleCount() throws Throwable {
		log.info("testSortedSortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedSampleForEach() throws Throwable {
		log.info("testSortedSortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testSortedSortedSample After---------------------------------------");
	}

	@Test
	public void testSortedSortedSortedCollect() throws Throwable {
		log.info("testSortedSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedSorted After---------------------------------------");
	}

	@Test
	public void testSortedSortedSortedCount() throws Throwable {
		log.info("testSortedSortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.sorted(new SortedComparator<String>() {
					public int compare(String value1, String value2) {
						return value1.compareTo(value2);
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

		log.info("testSortedSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testSortedSortedSortedForEach() throws Throwable {
		log.info("testSortedSortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.sorted(new SortedComparator<String>() {
			public int compare(String value1, String value2) {
				return value1.compareTo(value2);
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

		log.info("testSortedSortedSorted After---------------------------------------");
	}
}
