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
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineFoldByKeyKeyByTest extends StreamPipelineBaseTestCommon {

	boolean toexecute = true;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFoldByKey() throws Throwable {
		log.info("{}","testFoldByKey Before---------------------------------------");
		Long foldValue = 0l;
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(str -> Tuple.tuple(Integer.parseInt(str[1]), Long.parseLong(str[14]))).foldRight(foldValue, (a, b) -> a + b, 1, (a, b) -> a + b)
				.sorted((value1, value2) -> {
					return ((Integer) value1.v1).compareTo((Integer) value2.v1);
				}).collect(toexecute, new SampleSupplierPartition(3));
		long sum = 0;
		for (List<Tuple>  tuples :tupleslist) {
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("{}","");
		}
		assertEquals(-63278, sum);
		log.info("{}","testFoldByKey After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFoldByKeyFoldValue2() throws Throwable {
		log.info("{}","testFoldByKeyFoldValue2 Before---------------------------------------");
		Long foldValue = 2l;
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(str -> Tuple.tuple(str[1], Long.parseLong(str[14]))).foldLeft(foldValue, (a, b) -> a + b, 1, (a, b) -> a + b)
				.collect(toexecute, null);
		List<List<Long>> counts = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).
				map(str -> str[1]).distinct().mapToPair(dat -> new Tuple2<String, Long>(dat, 1l)).reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b)
				.map(tup -> (String) tup.v1).distinct().count(null);
		long sum = 0;
		for (List<Tuple>  tuples :tupleslist) {
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("{}","");
		}
		assertEquals(-63278 + counts.get(0).get(0) * foldValue, sum);
		log.info("{}","testFoldByKeyFoldValue2 After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyBy() throws Throwable {
		log.info("{}","testKeyBy Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List<Tuple2>> tupleslist = (List) datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2)).reduceByKey((a, b) -> a + "," + b).coalesce(1, (a, b) -> a + "," + b)
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :tupleslist) {
			sum += tuples.size();
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(79, sum);
		log.info("{}","testKeyBy After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyBySorted() throws Throwable {
		log.info("{}","testKeyBySorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List<Tuple2>> tupleslist = (List) datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2)).reduceByKey((a, b) -> a + "," + b).coalesce(1, (a, b) -> a + "," + b)
				.sorted((val1, val2) -> ((String) val1.v1).compareToIgnoreCase((String) val2.v1))
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :tupleslist) {
			sum += tuples.size();
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(79, sum);
		log.info("{}","testKeyBySorted After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByJoin() throws Throwable {
		log.info("{}","testKeyByJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		MapPair<String, String> maptup = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2));
		MapPair<String, String> maptup2 = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2));
		List<List<Tuple2>> joins = maptup.join(maptup2, (a, b) -> a.equals(b)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :joins) {
			sum += tuples.size();
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(455, sum);
		log.info("{}","testKeyByJoin After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByJoinSorted() throws Throwable {
		log.info("{}","testKeyByJoinSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		MapPair<String, String> maptup = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2));
		MapPair<String, String> maptup2 = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2));
		List<List<Tuple2>> joins = maptup.join(maptup2, (a, b) -> a.equals(b))
				.sorted((Tuple2 val1, Tuple2 val2) -> ((String) ((Tuple2) val1.v1).v1).compareToIgnoreCase(((String) ((Tuple2) val2.v1).v1))).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :joins) {
			sum += tuples.size();
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(455, sum);
		log.info("{}","testKeyByJoinSorted After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByReduceByKeyJoinSorted() throws Throwable {
		log.info("{}","testKeyByReduceByKeyJoinSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		MapPair<String, String> maptup = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2)).reduceByKey((a, b) -> a + "," + b).coalesce(1, (a, b) -> a + "," + b);
		MapPair<String, String> maptup2 = datastream.map(str -> str.split(" "))
				.flatMap(val -> Arrays.asList(val).stream())
				.filter(val -> val.trim().length() >= 2)
				.keyBy(val -> val.substring(0, 2)).reduceByKey((a, b) -> a + "," + b).coalesce(1, (a, b) -> a + "," + b);
		List<List<Tuple2>> joins = maptup.join(maptup2, (a, b) -> a.equals(b))
				.sorted((Tuple2 val1, Tuple2 val2) -> ((String) ((Tuple2) val1.v1).v1).compareToIgnoreCase(((String) ((Tuple2) val2.v1).v1))).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :joins) {
			sum += tuples.size();
			for (Tuple tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(79, sum);
		log.info("{}","testKeyByReduceByKeyJoinSorted After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByFlatMapMultiValues() throws Throwable {
		log.info("{}","testKeyByFlatMapMultiValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List<Tuple2>> results = datastream.map(str -> str.split("\n"))
				.flatMap(val -> Arrays.asList(val).stream())
				.map(str -> str.split(" "))
				.filter(val -> val[0].trim().length() >= 2)
				.keyBy(val1 -> val1[0].substring(0, 2))
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :results) {
			sum += tuples.size();
			for (Tuple2 tuple :tuples) {
				log.info("{}",tuple.v1 + " " + Arrays.asList((String[]) tuple.v2));
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(22, sum);
		log.info("{}","testKeyByFlatMapMultiValues After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByFlatMapJoinMultiValues() throws Throwable {
		log.info("{}","testKeyByFlatMapJoinMultiValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		MapPair<String, String[]> map1 = datastream.map(str -> str.split("\n"))
				.flatMap(val -> Arrays.asList(val).stream())
				.map(str -> str.split(" "))
				.filter(val -> val[0].trim().length() >= 2)
				.keyBy(val1 -> val1[0].substring(0, 2));
		MapPair<String, String[]> map2 = datastream.map(str -> str.split("\n"))
				.flatMap(val -> Arrays.asList(val).stream())
				.map(str -> str.split(" "))
				.filter(val -> val[0].trim().length() >= 3)
				.keyBy(val1 -> val1[0].substring(0, 3));
		List<List<Tuple2>> results = map2.join(map1, (a, b) -> a.v1
				.startsWith(b.v1)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :results) {
			sum += tuples.size();
			for (Tuple2 tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(15, sum);
		log.info("{}","testKeyByFlatMapJoinMultiValues After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testKeyByFlatMapLeftOuterJoinMultiValues() throws Throwable {
		log.info("{}","testKeyByFlatMapLeftOuterJoinMultiValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		MapPair<String, String[]> map1 = datastream.map(str -> str.split("\n"))
				.flatMap(val -> Arrays.asList(val).stream())
				.map(str -> str.split(" "))
				.filter(val -> val[0].trim().length() >= 2)
				.keyBy(val1 -> val1[0].substring(0, 2));
		MapPair<String, String[]> map2 = datastream.map(str -> str.split("\n"))
				.flatMap(val -> Arrays.asList(val).stream())
				.map(str -> str.split(" "))
				.filter(val -> val[0].trim().length() >= 3)
				.keyBy(val1 -> val1[0].substring(0, 3));
		List<List<Tuple2>> results = map2.leftOuterjoin(map1, (a, b) -> a.v1
				.startsWith(b.v1)).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2>  tuples :results) {
			sum += tuples.size();
			for (Tuple2 tuple :tuples) {
				log.info("{}",tuple);
			}
			log.info("{}","");
		}
		log.info("{}",sum);
		assertEquals(15, sum);
		List<List<Tuple2>> keybyvalues = map1.collect(toexecute, null);
		log.info("{}","");
		for (List<Tuple2> tuples : keybyvalues) {
			for (Tuple2 tuple : tuples) {
				log.info("{}",tuple);
			}
		}

		List<List<Tuple2>> keybyvaluesmap2 = map2.collect(toexecute, null);
		log.info("{}","");
		for (List<Tuple2> tuples : keybyvaluesmap2) {
			for (Tuple2 tuple : tuples) {
				log.info("{}",tuple);
			}
		}

		log.info("{}","testKeyByFlatMapLeftOuterJoinMultiValues After---------------------------------------");
	}
}
