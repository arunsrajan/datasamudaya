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
import java.util.List;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineDepth32ContinuedTest extends StreamPipelineBaseTestCommon {
	boolean toexecute = true;
	Logger log = Logger.getLogger(StreamPipelineDepth32ContinuedTest.class);
	int sum;


	@Test
	public void testMapPairMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info(
				"testMapPairMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info(
				"testMapPairMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testMapPairMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSampleCollect() throws Throwable {
		log.info("testMapPairMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSampleCount() throws Throwable {
		log.info("testMapPairMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(46361, sum);

		log.info("testMapPairMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSampleForEach() throws Throwable {
		log.info("testMapPairMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSortedCollect() throws Throwable {
		log.info("testMapPairMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSortedCount() throws Throwable {
		log.info("testMapPairMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairMapPairSortedForEach() throws Throwable {
		log.info("testMapPairMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFilterCollect() throws Throwable {
		log.info("testMapPairPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
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

		log.info("testMapPairPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFilterCount() throws Throwable {
		log.info("testMapPairPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
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

		log.info("testMapPairPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFilterForEach() throws Throwable {
		log.info("testMapPairPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFlatMapCollect() throws Throwable {
		log.info("testMapPairPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFlatMapCount() throws Throwable {
		log.info("testMapPairPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekFlatMapForEach() throws Throwable {
		log.info("testMapPairPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapCollect() throws Throwable {
		log.info("testMapPairPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapCount() throws Throwable {
		log.info("testMapPairPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapForEach() throws Throwable {
		log.info("testMapPairPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairCollect() throws Throwable {
		log.info("testMapPairPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairCount() throws Throwable {
		log.info("testMapPairPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairForEach() throws Throwable {
		log.info("testMapPairPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairPeekPeekCollect() throws Throwable {
		log.info("testMapPairPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairPeekPeekCount() throws Throwable {
		log.info("testMapPairPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
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

		log.info("testMapPairPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekPeekForEach() throws Throwable {
		log.info("testMapPairPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSampleCollect() throws Throwable {
		log.info("testMapPairPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSampleCount() throws Throwable {
		log.info("testMapPairPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
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

		log.info("testMapPairPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSampleForEach() throws Throwable {
		log.info("testMapPairPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSortedCollect() throws Throwable {
		log.info("testMapPairPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSortedCount() throws Throwable {
		log.info("testMapPairPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairPeekSortedForEach() throws Throwable {
		log.info("testMapPairPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFilterMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFilterSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFilterSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
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

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyFlatMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyFlatMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyFlatMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyFlatMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairReduceByKeyCount() throws Throwable {
		log.info(
				"testMapPairReduceByKeyFlatMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyFlatMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyFlatMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyFlatMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyFlatMapSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyFlatMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyFlatMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
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

		log.info("testMapPairReduceByKeyMapFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyMapFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyMapMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyMapPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyMapSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairGroupByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeyPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeySampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeySampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeySortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairGroupByKeySortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairGroupByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairGroupByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairReduceByKeyCount() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info(
				"testMapPairReduceByKeyMapPairMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFlatMapCount() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairCount() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				.count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKeyCollect() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKeyForEach() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKeyCollect() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKeyCount() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				.reduceByKey((a, b) -> a + b).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKeyForEach() throws Throwable {
		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info(
				"testMapPairReduceByKeyMapPairReduceByKeyMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeyPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeyPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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

		log.info("testMapPairReduceByKeyMapPairReduceByKeySampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeySample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				}).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeySortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairReduceByKeySortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairReduceByKeySorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairReduceByKeySorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyMapPairSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
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

		log.info("testMapPairReduceByKeyPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
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

		log.info("testMapPairReduceByKeyPeekFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyPeekFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeyPeekMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairReduceByKeyPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairReduceByKeyPeekMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairReduceByKeyPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
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

		log.info("testMapPairReduceByKeyPeekMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).peek(val -> System.out.println(val))
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val)).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeyPeekSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeyPeekSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).peek(val -> System.out.println(val))
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeyPeekSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
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

		log.info("testMapPairReduceByKeySampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
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

		log.info("testMapPairReduceByKeySampleFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeySampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
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

		log.info("testMapPairReduceByKeySampleMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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

		log.info("testMapPairReduceByKeySampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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

		log.info("testMapPairReduceByKeySampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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

		log.info("testMapPairReduceByKeySampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
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

		log.info("testMapPairReduceByKeySampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySamplePeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeySamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySamplePeekCount() throws Throwable {
		log.info("testMapPairReduceByKeySamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).peek(val -> System.out.println(val)).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySamplePeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySamplePeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeySamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).sample(46361).count(null);
		long sum = 0;
		for (List<Long> partitioneddata : data) {
			for (Long count : partitioneddata) {
				log.info(count);
				sum += count;
			}
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeySampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeySampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySampleSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeySampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b).sample(46361)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFilterCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFilterCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFilterForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFlatMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFlatMapCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedFlatMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairReduceByKeySortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairReduceByKeySortedMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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

		log.info("testMapPairReduceByKeySortedMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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

		log.info("testMapPairReduceByKeySortedMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedPeekCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedPeekCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedPeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedPeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedPeekForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedPeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedPeek After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSampleCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSampleCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSampleForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSample After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSortedCollect() throws Throwable {
		log.info("testMapPairReduceByKeySortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSortedCount() throws Throwable {
		log.info("testMapPairReduceByKeySortedSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
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
		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKeySortedSortedForEach() throws Throwable {
		log.info("testMapPairReduceByKeySortedSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).reduceByKey((a, b) -> a + b)
				.sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
					public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
						return value1.compareTo(value2);
					}
				}).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairReduceByKeySortedSorted After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFilterCollect() throws Throwable {
		log.info("testMapPairSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361)
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

		log.info("testMapPairSampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFilterCount() throws Throwable {
		log.info("testMapPairSampleFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361)
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

		log.info("testMapPairSampleFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFilterForEach() throws Throwable {
		log.info("testMapPairSampleFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361)
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
					public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
						return true;
					}
				}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSampleFilter After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFlatMapCollect() throws Throwable {
		log.info("testMapPairSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFlatMapCount() throws Throwable {
		log.info("testMapPairSampleFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleFlatMapForEach() throws Throwable {
		log.info("testMapPairSampleFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapCollect() throws Throwable {
		log.info("testMapPairSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapCount() throws Throwable {
		log.info("testMapPairSampleMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapForEach() throws Throwable {
		log.info("testMapPairSampleMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSampleMap After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairCollect() throws Throwable {
		log.info("testMapPairSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairCount() throws Throwable {
		log.info("testMapPairSampleMapPairCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPairCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairForEach() throws Throwable {
		log.info("testMapPairSampleMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.forEach(lis -> {
					sum += ((List) lis).size();
				}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSampleMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairGroupByKeyCollect() throws Throwable {
		log.info("testMapPairSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Tuple2>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairGroupByKeyForEach() throws Throwable {
		log.info("testMapPairSampleMapPairGroupByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairReduceByKeyCollect() throws Throwable {
		log.info("testMapPairSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairReduceByKeyCount() throws Throwable {
		log.info("testMapPairSampleMapPairReduceByKeyCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
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

		log.info("testMapPairSampleMapPairReduceByKeyCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleMapPairReduceByKeyForEach() throws Throwable {
		log.info("testMapPairSampleMapPairReduceByKey Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<org.jooq.lambda.tuple.Tuple2, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(org.jooq.lambda.tuple.Tuple2 value) {
						return (Tuple2<String, String>) value;
					}
				})
				.reduceByKey((a, b) -> a + b).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(2, sum);

		log.info("testMapPairSampleMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSamplePeekCollect() throws Throwable {
		log.info("testMapPairSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).peek(val -> System.out.println(val)).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairSamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairSamplePeekCount() throws Throwable {
		log.info("testMapPairSamplePeekCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
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

		log.info("testMapPairSamplePeekCount After---------------------------------------");
	}

	@Test
	public void testMapPairSamplePeekForEach() throws Throwable {
		log.info("testMapPairSamplePeek Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).peek(val -> System.out.println(val)).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSamplePeek After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSampleCollect() throws Throwable {
		log.info("testMapPairSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).sample(46361).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);

		log.info("testMapPairSampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSampleCount() throws Throwable {
		log.info("testMapPairSampleSampleCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
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

		log.info("testMapPairSampleSampleCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSampleForEach() throws Throwable {
		log.info("testMapPairSampleSample Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).sample(46361).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSampleSample After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSortedCollect() throws Throwable {
		log.info("testMapPairSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairSampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSortedCount() throws Throwable {
		log.info("testMapPairSampleSortedCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Long>> data = (List) datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
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

		log.info("testMapPairSampleSortedCount After---------------------------------------");
	}

	@Test
	public void testMapPairSampleSortedForEach() throws Throwable {
		log.info("testMapPairSampleSorted Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		sum = 0;
		datapipeline.mapToPair(
				new com.github.datasamudaya.common.functions.MapToPairFunction<java.lang.String, org.jooq.lambda.tuple.Tuple2<java.lang.String, java.lang.String>>() {
					public org.jooq.lambda.tuple.Tuple2 apply(java.lang.String value) {
						return (Tuple2<String, String>) Tuple.tuple(value.split(",")[8], value.split(",")[14]);
					}
				}).sample(46361).sorted(new com.github.datasamudaya.common.functions.SortedComparator<org.jooq.lambda.tuple.Tuple2>() {
			public int compare(org.jooq.lambda.tuple.Tuple2 value1, org.jooq.lambda.tuple.Tuple2 value2) {
				return value1.compareTo(value2);
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSampleSorted After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFilterCollect() throws Throwable {
		log.info("testMapPairSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFilterCount() throws Throwable {
		log.info("testMapPairSortedFilterCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedFilterCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFilterForEach() throws Throwable {
		log.info("testMapPairSortedFilter Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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
		}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<org.jooq.lambda.tuple.Tuple2>() {
			public boolean test(org.jooq.lambda.tuple.Tuple2 value) {
				return true;
			}
		}).forEach(lis -> {
			sum += ((List) lis).size();
		}, null);

		assertEquals(46361, sum);

		log.info("testMapPairSortedFilter After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFlatMapCollect() throws Throwable {
		log.info("testMapPairSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFlatMapCount() throws Throwable {
		log.info("testMapPairSortedFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedFlatMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedFlatMapForEach() throws Throwable {
		log.info("testMapPairSortedFlatMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapCollect() throws Throwable {
		log.info("testMapPairSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapCount() throws Throwable {
		log.info("testMapPairSortedMapCount Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedMapCount After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapForEach() throws Throwable {
		log.info("testMapPairSortedMap Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedMap After---------------------------------------");
	}

	@Test
	public void testMapPairSortedMapPairCollect() throws Throwable {
		log.info("testMapPairSortedMapPair Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
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

		log.info("testMapPairSortedMapPair After---------------------------------------");
	}

}
