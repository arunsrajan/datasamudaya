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

import com.github.datasamudaya.stream.IgnitePipeline;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class IgnitePipelineDepth32Test extends StreamPipelineIgniteBase {
	boolean toexecute = true;
	int sum;
	static Logger log = Logger.getLogger(IgnitePipelineDepth32Test.class);
	@Test
	public void testMapPeekSampleForEach() throws Throwable {
		log.info("testMapPeekSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPeekSortedCollect() throws Throwable {
		log.info("testMapPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPeekSortedCount() throws Throwable {
		log.info("testMapPeekSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).peek(val -> System.out.println(val)).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
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

		log.info("testMapPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPeekSortedForEach() throws Throwable {
		log.info("testMapPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).peek(val -> System.out.println(val)).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapSampleFilterCollect() throws Throwable {
		log.info("testMapSampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapSampleFilter After---------------------------------------");
	}

	@Test
	public void testMapSampleFilterCount() throws Throwable {
		log.info("testMapSampleFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
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

		log.info("testMapSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testMapSampleFilterForEach() throws Throwable {
		log.info("testMapSampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapSampleFilter After---------------------------------------");
	}

	@Test
	public void testMapSampleFlatMapCollect() throws Throwable {
		log.info("testMapSampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361)
				.flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
					public Stream<java.lang.String> apply(java.lang.String[] value) {
						return Arrays.asList(value[8] + "-" + value[14]).stream();
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapSampleFlatMapCount() throws Throwable {
		log.info("testMapSampleFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361)
				.flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
					public Stream<java.lang.String> apply(java.lang.String[] value) {
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

		log.info("testMapSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapSampleFlatMapForEach() throws Throwable {
		log.info("testMapSampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public Stream<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapSampleMapCollect() throws Throwable {
		log.info("testMapSampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSampleMap After---------------------------------------");
	}

	@Test
	public void testMapSampleMapCount() throws Throwable {
		log.info("testMapSampleMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
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

		log.info("testMapSampleMapCount After---------------------------------------");
	}

	@Test
	public void testMapSampleMapForEach() throws Throwable {
		log.info("testMapSampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleMap After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairCollect() throws Throwable {
		log.info("testMapSampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairCount() throws Throwable {
		log.info("testMapSampleMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairForEach() throws Throwable {
		log.info("testMapSampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapSampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapSampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapSampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapSampleMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapSampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapSamplePeekCollect() throws Throwable {
		log.info("testMapSamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSamplePeek After---------------------------------------");
	}

	@Test
	public void testMapSamplePeekCount() throws Throwable {
		log.info("testMapSamplePeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
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

		log.info("testMapSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testMapSamplePeekForEach() throws Throwable {
		log.info("testMapSamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSamplePeek After---------------------------------------");
	}

	@Test
	public void testMapSampleSampleCollect() throws Throwable {
		log.info("testMapSampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSampleSample After---------------------------------------");
	}

	@Test
	public void testMapSampleSampleCount() throws Throwable {
		log.info("testMapSampleSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
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

		log.info("testMapSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testMapSampleSampleForEach() throws Throwable {
		log.info("testMapSampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleSample After---------------------------------------");
	}

	@Test
	public void testMapSampleSortedCollect() throws Throwable {
		log.info("testMapSampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSampleSorted After---------------------------------------");
	}

	@Test
	public void testMapSampleSortedCount() throws Throwable {
		log.info("testMapSampleSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
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

		log.info("testMapSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testMapSampleSortedForEach() throws Throwable {
		log.info("testMapSampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSampleSorted After---------------------------------------");
	}

	@Test
	public void testMapSortedFilterCollect() throws Throwable {
		log.info("testMapSortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testMapSortedFilter After---------------------------------------");
	}

	@Test
	public void testMapSortedFilterCount() throws Throwable {
		log.info("testMapSortedFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
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

		log.info("testMapSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testMapSortedFilterForEach() throws Throwable {
		log.info("testMapSortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String[]>() {
			public boolean test(java.lang.String[] value) {
				return !"NA".equals(value[14]) && !"ArrDelay".equals(value[14]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(45957, sum);

		log.info("testMapSortedFilter After---------------------------------------");
	}

	@Test
	public void testMapSortedFlatMapCollect() throws Throwable {
		log.info("testMapSortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public Stream<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapSortedFlatMapCount() throws Throwable {
		log.info("testMapSortedFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public Stream<java.lang.String> apply(java.lang.String[] value) {
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

		log.info("testMapSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapSortedFlatMapForEach() throws Throwable {
		log.info("testMapSortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).flatMap(new com.github.datasamudaya.common.functions.FlatMapFunction<java.lang.String[], java.lang.String>() {
			public Stream<java.lang.String> apply(java.lang.String[] value) {
				return Arrays.asList(value[8] + "-" + value[14]).stream();
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapSortedMapCollect() throws Throwable {
		log.info("testMapSortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedMap After---------------------------------------");
	}

	@Test
	public void testMapSortedMapCount() throws Throwable {
		log.info("testMapSortedMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
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

		log.info("testMapSortedMapCount After---------------------------------------");
	}

	@Test
	public void testMapSortedMapForEach() throws Throwable {
		log.info("testMapSortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String[], java.lang.String>() {
			public java.lang.String apply(java.lang.String[] value) {
				return value[8] + "-" + value[14];
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedMap After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairCollect() throws Throwable {
		log.info("testMapSortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairCount() throws Throwable {
		log.info("testMapSortedMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairForEach() throws Throwable {
		log.info("testMapSortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapSortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				})
				.reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapSortedMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
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

		log.info("testMapSortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapSortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapSortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String[], org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String[] value) {
						return (Tuple2<String, String>) Tuple.tuple(value[8], value[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapSortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapSortedPeekCollect() throws Throwable {
		log.info("testMapSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedPeek After---------------------------------------");
	}

	@Test
	public void testMapSortedPeekCount() throws Throwable {
		log.info("testMapSortedPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
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

		log.info("testMapSortedPeekCount After---------------------------------------");
	}

	@Test
	public void testMapSortedPeekForEach() throws Throwable {
		log.info("testMapSortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedPeek After---------------------------------------");
	}

	@Test
	public void testMapSortedSampleCollect() throws Throwable {
		log.info("testMapSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedSample After---------------------------------------");
	}

	@Test
	public void testMapSortedSampleCount() throws Throwable {
		log.info("testMapSortedSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
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

		log.info("testMapSortedSampleCount After---------------------------------------");
	}

	@Test
	public void testMapSortedSampleForEach() throws Throwable {
		log.info("testMapSortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedSample After---------------------------------------");
	}

	@Test
	public void testMapSortedSortedCollect() throws Throwable {
		log.info("testMapSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapSortedSorted After---------------------------------------");
	}

	@Test
	public void testMapSortedSortedCount() throws Throwable {
		log.info("testMapSortedSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline
				.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
					public java.lang.String[] apply(java.lang.String value) {
						return value.split(",");
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
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

		log.info("testMapSortedSortedCount After---------------------------------------");
	}

	@Test
	public void testMapSortedSortedForEach() throws Throwable {
		log.info("testMapSortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.map(new com.github.datasamudaya.common.functions.MapFunction<java.lang.String, java.lang.String[]>() {
			public java.lang.String[] apply(java.lang.String value) {
				return value.split(",");
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<java.lang.String[]>() {
			public int compare(java.lang.String[] value1, java.lang.String[] value2) {
				return value1[1].compareTo(value2[1]);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapSortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFilterCollect() throws Throwable {
		log.info("testMapPairFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFilterCount() throws Throwable {
		log.info("testMapPairFilterFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFilterForEach() throws Throwable {
		log.info("testMapPairFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFlatMapCollect() throws Throwable {
		log.info("testMapPairFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFlatMapCount() throws Throwable {
		log.info("testMapPairFilterFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterFlatMapForEach() throws Throwable {
		log.info("testMapPairFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapCollect() throws Throwable {
		log.info("testMapPairFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapCount() throws Throwable {
		log.info("testMapPairFilterMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapForEach() throws Throwable {
		log.info("testMapPairFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairCollect() throws Throwable {
		log.info("testMapPairFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairCount() throws Throwable {
		log.info("testMapPairFilterMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairForEach() throws Throwable {
		log.info("testMapPairFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairFilterMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFilterPeekCollect() throws Throwable {
		log.info("testMapPairFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairFilterPeekCount() throws Throwable {
		log.info("testMapPairFilterPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
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

		log.info("testMapPairFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterPeekForEach() throws Throwable {
		log.info("testMapPairFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSampleCollect() throws Throwable {
		log.info("testMapPairFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSampleCount() throws Throwable {
		log.info("testMapPairFilterSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
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

		log.info("testMapPairFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSampleForEach() throws Throwable {
		log.info("testMapPairFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSortedCollect() throws Throwable {
		log.info("testMapPairFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSortedCount() throws Throwable {
		log.info("testMapPairFilterSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairFilterSortedForEach() throws Throwable {
		log.info("testMapPairFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFilterCollect() throws Throwable {
		log.info("testMapPairFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFilterCount() throws Throwable {
		log.info("testMapPairFlatMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFilterForEach() throws Throwable {
		log.info("testMapPairFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFlatMapCollect() throws Throwable {
		log.info("testMapPairFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFlatMapCount() throws Throwable {
		log.info("testMapPairFlatMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapFlatMapForEach() throws Throwable {
		log.info("testMapPairFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapCollect() throws Throwable {
		log.info("testMapPairFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapCount() throws Throwable {
		log.info("testMapPairFlatMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapForEach() throws Throwable {
		log.info("testMapPairFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairCollect() throws Throwable {
		log.info("testMapPairFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairCount() throws Throwable {
		log.info("testMapPairFlatMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairForEach() throws Throwable {
		log.info("testMapPairFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapPeekCollect() throws Throwable {
		log.info("testMapPairFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapPeekCount() throws Throwable {
		log.info("testMapPairFlatMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapPeekForEach() throws Throwable {
		log.info("testMapPairFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSampleCollect() throws Throwable {
		log.info("testMapPairFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSampleCount() throws Throwable {
		log.info("testMapPairFlatMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSampleForEach() throws Throwable {
		log.info("testMapPairFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSortedCollect() throws Throwable {
		log.info("testMapPairFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSortedCount() throws Throwable {
		log.info("testMapPairFlatMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapSortedForEach() throws Throwable {
		log.info("testMapPairFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFilterSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFilterSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyFlatMapSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyFlatMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey()
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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
		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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
		assertEquals(2, sum);

		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info(
				"testMapPairGroupByKeyMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info(
				"testMapPairGroupByKeyMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairReduceByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyMapPairSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyMapPairSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val))
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val))
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeyPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeyPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeyPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val)).sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeyPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeyPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeyPeekSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeyPeekSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeyPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeySampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairGroupByKeySampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeySampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySamplePeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeySamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySamplePeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeySamplePeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361).sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeySampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeySampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySampleSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeySampleSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sample(46361)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedFilterCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedFilterForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedFlatMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeySortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedFlatMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeySortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairGroupByKeySortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairGroupByKeySortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedPeekCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedPeekForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedSampleCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> lsttuples : data) {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedSampleForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedSortedCollect() throws Throwable {
		log.info("testMapPairGroupByKeySortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairGroupByKeySortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKeySortedSortedForEach() throws Throwable {
		log.info("testMapPairGroupByKeySortedSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairGroupByKeySortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapFilterCollect() throws Throwable {
		log.info("testMapPairMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapFilterCount() throws Throwable {
		log.info("testMapPairMapFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapFilterForEach() throws Throwable {
		log.info("testMapPairMapFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapFlatMapCollect() throws Throwable {
		log.info("testMapPairMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapFlatMapCount() throws Throwable {
		log.info("testMapPairMapFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapFlatMapForEach() throws Throwable {
		log.info("testMapPairMapFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapCollect() throws Throwable {
		log.info("testMapPairMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapCount() throws Throwable {
		log.info("testMapPairMapMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapForEach() throws Throwable {
		log.info("testMapPairMapMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairCollect() throws Throwable {
		log.info("testMapPairMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairCount() throws Throwable {
		log.info("testMapPairMapMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairForEach() throws Throwable {
		log.info("testMapPairMapMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairMapMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairMapMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairMapMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPeekCollect() throws Throwable {
		log.info("testMapPairMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPeekCount() throws Throwable {
		log.info("testMapPairMapPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPeekForEach() throws Throwable {
		log.info("testMapPairMapPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapSampleCollect() throws Throwable {
		log.info("testMapPairMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairMapSampleCount() throws Throwable {
		log.info("testMapPairMapSampleCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapSampleForEach() throws Throwable {
		log.info("testMapPairMapSample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairMapSortedCollect() throws Throwable {
		log.info("testMapPairMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapSortedCount() throws Throwable {
		log.info("testMapPairMapSortedCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapSortedForEach() throws Throwable {
		log.info("testMapPairMapSorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFilterCollect() throws Throwable {
		log.info("testMapPairMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFilterCount() throws Throwable {
		log.info("testMapPairMapPairFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFilterForEach() throws Throwable {
		log.info("testMapPairMapPairFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFlatMapCollect() throws Throwable {
		log.info("testMapPairMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFlatMapCount() throws Throwable {
		log.info("testMapPairMapPairFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairFlatMapForEach() throws Throwable {
		log.info("testMapPairMapPairFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lsttuples -> {
					for (Tuple2 tuple2 : lsttuples) {
						sum += ((List) tuple2.v2).size();
					}

				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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

		log.info("testMapPairMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeyPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().peek(val -> System.out.println(val)).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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

		log.info("testMapPairMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeySample Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sample(46361).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testMapPairMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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

		log.info("testMapPairMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testMapPairMapPairGroupByKeySorted Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.groupByKey().sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapCollect() throws Throwable {
		log.info("testMapPairMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapCount() throws Throwable {
		log.info("testMapPairMapPairMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapForEach() throws Throwable {
		log.info("testMapPairMapPairMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairCollect() throws Throwable {
		log.info("testMapPairMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairCount() throws Throwable {
		log.info("testMapPairMapPairMapPairCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairForEach() throws Throwable {
		log.info("testMapPairMapPairMapPair Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairMapPairGroupByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.groupByKey().forEach(lsttuples -> {
			for (Tuple2 tuple2 : lsttuples) {
				sum += ((List) tuple2.v2).size();
			}

		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairMapPairMapPairReduceByKeyCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairMapPairReduceByKey Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.mapToPair(
						new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
							public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
								return (Tuple2<String, String>) value;
							}
						})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairPeekCollect() throws Throwable {
		log.info("testMapPairMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairPeekCount() throws Throwable {
		log.info("testMapPairMapPairPeekCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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

		log.info("testMapPairMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairPeekForEach() throws Throwable {
		log.info("testMapPairMapPairPeek Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFilterCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFilter Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyFlatMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapCount Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
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

		log.info("testMapPairMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMap Before---------------------------------------");
		IgnitePipeline<String> datapipeline = IgnitePipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyMap After---------------------------------------");
	}

}
