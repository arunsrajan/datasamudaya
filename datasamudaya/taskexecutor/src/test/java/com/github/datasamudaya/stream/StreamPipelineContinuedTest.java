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

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple4;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.functions.MapFunction;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineContinuedTest extends StreamPipelineBaseTestCommon {

	boolean toexecute = true;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterCount() throws Throwable {
		log.info("testFilterCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> filterresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1])).count(null);
		filterresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> frs : filterresult) {
			for (Long fr : frs) {
				log.info(fr);
				sum += fr;
			}
		}
		log.info(sum);
		assertEquals(7759, sum);
		log.info("testFilterCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})

	@Test
	public void testFilterCountPartitioned() throws Throwable {
		log.info("testFilterCountPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> filterresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]))
				.count(new NumPartitions(4));
		filterresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> frs : filterresult) {
			for (Long fr : frs) {
				log.info(fr);
				sum += fr;
			}
		}
		assertEquals(7759, sum);
		log.info("testFilterCountPartitioned After---------------------------------------");
	}

	@Test
	public void testFilterForEach() throws Throwable {
		log.info("testFilterForEach Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.filter(dat -> "1".equals(dat.split(",")[3])).forEach(System.out::println, new NumPartitions(4));
		log.info("testFilterForEach After---------------------------------------");
	}

	@Test
	public void testFilterForEachPeek() throws Throwable {
		log.info("testFilterForEachPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.filter(dat -> "1".equals(dat.split(",")[3])).peek(val -> System.out.println(val)).forEach(System.out::println,
				new NumPartitions(4));
		log.info("testFilterForEachPeek After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapCount() throws Throwable {
		log.info("testFilterMapCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]))
				.map(dat -> dat.split(",")).count(null);
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(7759, sum);
		log.info("testFilterMapCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapCountPartitioned() throws Throwable {
		log.info("testFilterMapCountPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]))
				.map(dat -> dat.split(",")).count(new NumPartitions(4));
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(7759, sum);
		log.info("testFilterMapCountPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapPairCount() throws Throwable {
		log.info("testFilterMapPairCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]))
				.map(dat -> dat.split(",")).mapToPair(datarr -> Tuple.tuple(datarr[8], datarr[14])).count(null);
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(7759, sum);
		log.info("testFilterMapPairCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterMapPairCountPartitioned() throws Throwable {
		log.info("testFilterMapPairCountPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream
				.filter(dat -> "1".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]))
				.map(dat -> dat.split(",")).mapToPair(datarr -> Tuple.tuple(datarr[8], datarr[14]))
				.count(new NumPartitions(4));
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(7759, sum);
		log.info("testFilterMapPairCountPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testFilterMultipleSortFieldsMapPartitioned() throws Throwable {
		log.info("testFilterMultipleSortFieldsMapPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline map = (StreamPipeline) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]);
		}).sorted((val1, val2) -> {
			String[] vals1 = ((String) val1).split(",");
			String[] vals2 = ((String) val2).split(",");
			Long vall1 = Long.parseLong(vals1[1]);
			Long vall2 = Long.parseLong(vals2[1]);
			int compresult = vall1.compareTo(vall2);
			if (compresult == 0) {
				vall1 = Long.parseLong(vals1[2]);
				vall2 = Long.parseLong(vals2[2]);
				compresult = vall1.compareTo(vall2);
				if (compresult == 0) {
					vall1 = Long.parseLong(vals1[3]);
					vall2 = Long.parseLong(vals2[3]);
					return vall1.compareTo(vall2);
				}
				return compresult;
			}
			return compresult;
		}).map(vals -> {
			String[] valu = vals.split(",");
			return Tuple.tuple(valu[1], valu[2], valu[3], Long.parseLong(valu[14]));
		});
		List<List<Tuple4>> values = (List) map.collect(toexecute, new NumPartitions(3));
		assertEquals(3, values.size());
		long sum = 0;
		for (List<Tuple4> vals : values) {
			for (Tuple4 value : vals) {
				sum += (long) value.v4;
				log.info(value);
			}
			log.info("\n");
		}
		assertTrue(sum == -63278);
		log.info("testFilterMultipleSortFieldsMapPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testFilterPartitioned() throws Throwable {
		log.info("testFilterPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> resultleft = (List) datastream.filter(dat -> "21".equals(dat.split(",")[2])).collect(toexecute,
				null);
		List<List> resultright = (List) datastream.filter(dat -> "21".equals(dat.split(",")[2])).collect(toexecute,
				new NumPartitions(3));
		assertEquals(1, resultleft.size());
		assertEquals(1, resultright.size());
		int sumleft = 0;
		int sumright = 0;
		for (List<Long> vals : resultleft) {
			sumleft += vals.size();
		}
		for (List<Long> vals : resultright) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);
		log.info("testFilterPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testFilterSortedPartitioned() throws Throwable {
		log.info("testFilterSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline map = datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]);
		}).sorted((val1, val2) -> {
			String[] vals1 = ((String) val1).split(",");
			String[] vals2 = ((String) val2).split(",");
			Long vall1 = Long.parseLong(vals1[2]);
			Long vall2 = Long.parseLong(vals2[2]);
			return vall1.compareTo(vall2);
		});
		List<List<String>> values = (List) map.collect(toexecute, new NumPartitions(3));
		assertEquals(3, values.size());
		int totalrecords = 0;
		for (List<String> vals : values) {
			totalrecords += vals.size();
			for (String valarr : vals) {
				log.info(valarr);
			}
		}
		assertEquals(46360, totalrecords);
		log.info("testFilterSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFlatMapCount() throws Throwable {
		log.info("testFlatMapCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.flatMap(str -> Arrays.asList(str.split(",")).stream()).count(null);
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(1344469, sum);
		log.info("testFlatMapCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFlatMapCountPartitioned() throws Throwable {
		log.info("testFlatMapCountPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.flatMap(str -> Arrays.asList(str.split(",")).stream())
				.count(new NumPartitions(4));
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(1344469, sum);
		log.info("testFlatMapCountPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFlatMapCountSmallBlocks() throws Throwable {
		log.info("testFlatMapCountSmallBlocks Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.flatMap(str -> Arrays.asList(str.split(",")).stream()).count(null);
		int sum = 0;
		for (List<Long> vals : mapresult) {
			for (Long valarr : vals) {
				log.info(valarr);
				sum += valarr;
			}
		}
		assertEquals(1344469, sum);
		log.info("testFlatMapCountSmallBlocks After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFlatMapCountSmallBlocksPartitioned() throws Throwable {
		log.info("testFlatMapCountSmallBlocks Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.flatMap(str -> Arrays.asList(str.split(",")).stream())
				.count(new NumPartitions(4));
		int sum = 0;
		assertEquals(1, mapresult.size());
		for (List<Long> vals : mapresult) {
			for (Long valarr : vals) {
				log.info(valarr);
				sum += valarr;
			}
		}
		assertEquals(1344469, sum);
		log.info("testFlatMapCountSmallBlocks After---------------------------------------");
	}

	public void testForEachMap() throws Throwable {
		log.info("testForEachMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<Object> map = datastream.map(dat -> dat.split(","));
		map.forEach(System.out::println, null);
		log.info("testForEachMap After---------------------------------------");
	}

	public void testForEachMapPartitioned() throws Throwable {
		log.info("testForEachMapPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<Object> map = datastream.map(dat -> dat.split(","));
		map.forEach(System.out::println, new NumPartitions(4));
		log.info("testForEachMapPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapCount() throws Throwable {
		log.info("testMapCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.map(dat -> dat.split(",")).count(null);
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(46361, sum);
		log.info("testMapCount After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapCountPartitioned() throws Throwable {
		log.info("testMapCountPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Long>> mapresult = (List) datastream.map(dat -> dat.split(",")).count(new NumPartitions(4));
		mapresult.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> mrs : mapresult) {
			for (Long mr : mrs) {
				log.info(mr);
				sum += mr;
			}
		}
		assertEquals(46361, sum);
		log.info("testMapCountPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMapFilterSortedPartitioned() throws Throwable {
		log.info("testMapFilterSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline map = datastream.map(dat -> dat.split(",")).filter(dat -> !"ArrDelay".equals(dat[14]))
				.sorted((val1, val2) -> {
					Long vall1 = Long.parseLong(((String[]) val1)[2]);
					Long vall2 = Long.parseLong(((String[]) val2)[2]);
					return vall1.compareTo(vall2);
				});
		List<List<String[]>> values = (List) map.collect(toexecute, new NumPartitions(3));
		assertEquals(3, values.size());
		int sum = 0;
		for (List<String[]> vals : values) {
			sum += vals.size();
			for (String[] valarr : vals) {
				log.info(valarr[0] + " " + valarr[1] + " " + valarr[2]);
			}
		}
		assertEquals(46360, sum);
		log.info("testMapFilterSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"rawtypes", "unchecked", "serial"})
	@Test
	public void testMapPartitioned() throws Throwable {
		log.info("testMapPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List> resultleft = (List) datastream.map(new MapFunction<String, String[]>() {

			@Override
			public String[] apply(String dat) {
				return dat.split(",");
			}

		}).map(new MapFunction<String[], String>() {

			@Override
			public String apply(String[] dat) {
				return dat[0] + "" + dat[14];
			}

		}).collect(toexecute, null);
		List<List> resultright = (List) datastream.map(dat -> dat.split(",")).map(datas -> datas[0] + "" + datas[14])
				.collect(toexecute, new NumPartitions(2));
		assertEquals(1, resultleft.size());
		assertEquals(1, resultright.size());
		int sumleft = 0;
		int sumright = 0;
		for (List<Long> vals : resultleft) {
			sumleft += vals.size();
		}
		for (List<Long> vals : resultright) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);

		log.info("testMapPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMapPairSortedPartitioned() throws Throwable {
		log.info("testMapPairSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair mappair = (MapPair) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]);
		}).mapToPair(val -> {
			String[] valarr = val.split(",");
			return (Tuple2<String, Integer>) Tuple.tuple(valarr[8], Integer.parseInt(valarr[14]));
		}).sorted((val1, val2) -> {
			Tuple2<String, Integer> tup1 = (Tuple2) val1;
			Tuple2<String, Integer> tup2 = (Tuple2) val2;
			Integer vall1 = (Integer) tup1.v2;
			Integer vall2 = (Integer) tup2.v2;
			return vall1.compareTo(vall2);
		});
		List<List<Tuple2>> values = (List) mappair.collect(toexecute, new NumPartitions(2));
		int sum = 0;
		assertEquals(2, values.size());
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testMapPairSortedPeekPartitioned() throws Throwable {
		log.info("testMapPairSortedPeekPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair mappair = (MapPair) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]);
		}).mapToPair(val -> {
			String[] valarr = val.split(",");
			return (Tuple2) Tuple.tuple(valarr[8], Integer.parseInt(valarr[14]));
		}).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			Integer vall1 = (Integer) tup1.v2;
			Integer vall2 = (Integer) tup2.v2;
			return vall1.compareTo(vall2);
		});
		List<List<Tuple2>> values = (List) mappair.peek(val -> System.out.println(val)).collect(toexecute, new NumPartitions(2));
		int sum = 0;
		assertEquals(2, values.size());
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
			log.info("\n");
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSortedPeekPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMassiveDataPipelineSorted() throws Throwable {
		log.info("testMassiveDataPipelineSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplenoheader,
				pipelineconfig);
		List<List> sortedstream = (List) datastream.sorted((val1, val2) -> {
			String[] val1s = val1.split(",");
			String[] val2s = val2.split(",");
			int comparetoval = (Integer.valueOf(val1s[1])).compareTo((Integer.valueOf(val2s[1])));
			if (comparetoval == 0) {
				comparetoval = (Integer.valueOf(val1s[2])).compareTo((Integer.valueOf(val2s[2])));
				if (comparetoval == 0) {
					comparetoval = (Integer.valueOf(val1s[3])).compareTo((Integer.valueOf(val2s[3])));
					return comparetoval;
				}
				return comparetoval;
			}
			return comparetoval;
		}).collect(toexecute, null);
		assertEquals(1, sortedstream.size());
		for (List<String> vals : sortedstream) {
			for (String value : vals) {
				log.info(value);
			}
		}
		assertEquals(46360, sortedstream.get(0).size());
		log.info("testMassiveDataPipelineSorted After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMassiveDataPipelineSortedPartitioned() throws Throwable {
		log.info("testMassiveDataPipelineSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinenoheader,
				pipelineconfig);
		List<List> sortedpartitionedstream = (List) datastream.sorted((val1, val2) -> {
			String[] val1s = val1.split(",");
			String[] val2s = val2.split(",");
			int comparetoval = (Integer.valueOf(val1s[1])).compareTo((Integer.valueOf(val2s[1])));
			if (comparetoval == 0) {
				comparetoval = (Integer.valueOf(val1s[2])).compareTo((Integer.valueOf(val2s[2])));
				if (comparetoval == 0) {
					comparetoval = (Integer.valueOf(val1s[3])).compareTo((Integer.valueOf(val2s[3])));
					return comparetoval;
				}
				return comparetoval;
			}
			return comparetoval;
		}).collect(toexecute, new NumPartitions(4));
		assertEquals(1, sortedpartitionedstream.size());
		int recordcount = 0;
		for (List<String> vals : sortedpartitionedstream) {
			recordcount += vals.size();
			for (String value : vals) {
				log.info(value);
			}
		}
		assertEquals(29, recordcount);
		log.info("testMassiveDataPipelineSortedPartitioned After---------------------------------------");
	}

	@Test
	public void testMassiveDataPipelineSortedPartitionedExecutedDeferred() throws Throwable {
		log.info(
				"testMassiveDataPipelineSortedPartitionedExecutedDeferred Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplenoheader,
				pipelineconfig);
		datastream.sorted((val1, val2) -> {
			String[] val1s = val1.split(",");
			String[] val2s = val2.split(",");
			int comparetoval = (Integer.valueOf(val1s[1])).compareTo((Integer.valueOf(val2s[1])));
			if (comparetoval == 0) {
				comparetoval = (Integer.valueOf(val1s[2])).compareTo((Integer.valueOf(val2s[2])));
				if (comparetoval == 0) {
					comparetoval = (Integer.valueOf(val1s[3])).compareTo((Integer.valueOf(val2s[3])));
					return comparetoval;
				}
				return comparetoval;
			}
			return comparetoval;
		}).collect(false, new NumPartitions(4));
		log.info(
				"testMassiveDataPipelineSortedPartitionedExecutedDeferred After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMultipleDAGNoExecute() throws Throwable {
		log.info("testMultipleDAGNoExecute Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		StreamPipeline<Object> map1 = map.map(datas -> datas[0] + "" + datas[14]);
		List<List> lst1 = (List<List>) map1.collect(true, new NumPartitions(2));
		int sum = 0;
		for (List output : lst1) {
			sum += output.size();
		}
		assertEquals(46361, sum);
		StreamPipeline<String> map2 = map.map(datas -> datas[1] + "" + datas[14]);
		List<List> lst2 = (List<List>) map2.collect(true, new NumPartitions(3));
		sum = 0;
		for (List output : lst2) {
			sum += output.size();
		}
		assertEquals(46361, sum);
		log.info("testMultipleDAGNoExecute---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMultipleDAGPartitioned() throws Throwable {
		log.info("testMultipleDAGPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		StreamPipeline<Object> map1 = map.map(datas -> datas[0] + "" + datas[14]);
		StreamPipeline<Object> map2 = map.map(datas -> datas[1] + "" + datas[14]);
		StreamPipeline<Object> map3 = map.map(datas -> datas[2] + "" + datas[14]);
		StreamPipeline<Object> map4 = map.map(datas -> datas[3] + datas[14]);

		List<List> lst11 = (List<List>) map1.collect(toexecute, null);
		List<List> lst12 = (List<List>) map2.collect(toexecute, null);
		List<List> lst13 = (List<List>) map3.collect(toexecute, null);
		List<List> lst14 = (List<List>) map4.collect(toexecute, null);

		List<List> lst21 = (List<List>) map1.collect(toexecute, new NumPartitions(2));
		List<List> lst22 = (List<List>) map2.collect(toexecute, new NumPartitions(3));
		List<List> lst23 = (List<List>) map3.collect(toexecute, new NumPartitions(4));
		List<List> lst24 = (List<List>) map4.collect(toexecute, new NumPartitions(3));

		assertEquals(1, lst11.size());
		assertEquals(1, lst12.size());
		assertEquals(1, lst13.size());
		assertEquals(1, lst14.size());

		assertEquals(1, lst21.size());
		assertEquals(1, lst22.size());
		assertEquals(1, lst23.size());
		assertEquals(1, lst24.size());
		int sumleft = 0;
		int sumright = 0;
		for (List<Long> vals : lst11) {
			sumleft += vals.size();
		}
		for (List<Long> vals : lst21) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);
		sumleft = 0;
		sumright = 0;
		for (List<Long> vals : lst12) {
			sumleft += vals.size();
		}
		for (List<Long> vals : lst22) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);
		sumleft = 0;
		sumright = 0;
		for (List<Long> vals : lst13) {
			sumleft += vals.size();
		}
		for (List<Long> vals : lst23) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);
		sumleft = 0;
		sumright = 0;
		for (List<Long> vals : lst14) {
			sumleft += vals.size();
		}
		for (List<Long> vals : lst24) {
			sumright += vals.size();
		}
		assertEquals(sumleft, sumright);
		log.info("testMultipleDAGPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountCountAndWordSortedPartitioned() throws Throwable {
		log.info("testWordCountCountAndWordSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			int compres = ((Long) tup1.v2).compareTo(((Long) tup2.v2));
			if (compres == 0) {
				return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
			}
			return compres;
		}).collect(toexecute, new NumPartitions(2));
		int numberofwords = 0;
		Set<String> distinct = new HashSet<>();
		assertEquals(1, wordscount.size());
		for (List<Tuple2> vals : wordscount) {
			for (Tuple2 valarr : vals) {
				distinct.add((String) valarr.v1);
				numberofwords += (Long) valarr.v2;
				log.info(valarr);
			}
		}
		assertEquals(201, numberofwords);
		assertEquals(121, distinct.size());

		log.info("testWordCountCountAndWordSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountCounts() throws Throwable {
		log.info("testWordCountCounts Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List<Long>> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			int compres = ((Long) tup1.v2).compareTo(((Long) tup2.v2));
			if (compres == 0) {
				return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
			}
			return compres;
		}).count(null);
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> wcs : wordscount) {
			for (Long wc : wcs) {
				log.info(wc);
				sum += wc;
			}
		}
		assertEquals(121, sum);
		log.info("testWordCountCounts After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountCountSortedParitioned() throws Throwable {
		log.info("testWordCountCountSortedParitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			return ((Long) tup1.v2).compareTo(((Long) tup2.v2));
		}).collect(toexecute, new NumPartitions(2));
		int numberofwords = 0;
		Set<String> distinct = new HashSet<>();
		for (List<Tuple2> vals : wordscount) {
			for (Tuple2 valarr : vals) {
				distinct.add((String) valarr.v1);
				numberofwords += (Long) valarr.v2;
				log.info(valarr);
			}
		}
		assertEquals(201, numberofwords);
		assertEquals(121, distinct.size());

		log.info("testWordCountCountSortedParitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountCountsPartitioned() throws Throwable {
		log.info("testWordCountCountsPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List<Long>> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			int compres = ((Long) tup1.v2).compareTo(((Long) tup2.v2));
			if (compres == 0) {
				return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
			}
			return compres;
		}).count(new NumPartitions(4));
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);
		int sum = 0;
		for (List<Long> wcs : wordscount) {
			for (Long wc : wcs) {
				log.info(wc);
				sum += wc;
			}
		}
		assertEquals(121, sum);
		log.info("testWordCountCountsPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountWordSortedPartitioned() throws Throwable {
		log.info("testWordCountWordSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
		}).collect(toexecute, new NumPartitions(2));
		int numberofwords = 0;
		Set<String> distinct = new HashSet<>();
		assertEquals(1, wordscount.size());
		for (List<Tuple2> vals : wordscount) {
			for (Tuple2 valarr : vals) {
				distinct.add((String) valarr.v1);
				numberofwords += (Long) valarr.v2;
				log.info(valarr);
			}
		}
		assertEquals(201, numberofwords);
		assertEquals(121, distinct.size());
		log.info("testWordCountWordSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testWordCountWordSortedPartitionedForEach() throws Throwable {
		log.info("testWordCountWordSortedPartitionedForEach Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		MapPair<Tuple, Tuple> mappair = datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> (Tuple2) Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
		});

		mappair.forEach(System.out::println, new NumPartitions(4));
		mappair.forEach(System.out::println, new NumPartitions(3));
		log.info("testWordCountWordSortedPartitionedForEach After---------------------------------------");
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testWordCountWordSortedPartitionedJobStageTask() throws Throwable {
		log.info("testWordCountWordSortedPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		MapPair<String, Long> mappair = datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l))
				.reduceByKey((pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
		});

		List<List> output = (List<List>) mappair.collect(true, null);
		output.stream().flatMap(lst -> lst.stream()).forEach(System.out::println);
		log.info("testWordCountWordSortedPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCountByValue() throws Throwable {
		log.info("testCountByValue Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(str -> Tuple.tuple(str[8], str[14])).countByValue()
				.collect(toexecute, new NumPartitions(3));
		long sum = 0;
		for (List<Tuple> tuples : tupleslist) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(45957l, sum);
		log.info("testCountByValue After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCountByValueSorted() throws Throwable {
		log.info("testCountByValueSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(str -> Tuple.tuple(str[1], str[14])).countByValue()
				.reduceByKey((a, b) -> a + b).coalesce(1, (a, b) -> a + b).sorted((Tuple2 tup1, Tuple2 tup2) -> {
			return Integer.valueOf((String) ((Tuple2) tup1.v1).v1).compareTo(Integer.valueOf((String) ((Tuple2) tup2.v1).v1));
		}).collect(toexecute, new NumPartitions(3));
		long sum = 0;
		for (List<Tuple> tuples : tupleslist) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(45957l, sum);
		log.info("testCountByValueSorted After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduce() throws Throwable {
		log.info("testReduce Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> intlist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.map(val -> Integer.parseInt(val[14])).reduce((a, b) -> a + b).collect(toexecute, new NumPartitions(3));
		int sum = 0;
		for (List<Integer> values : intlist) {
			for (Integer value : values) {
				log.info(value);
				sum += value;
			}
			log.info("");
		}
		assertEquals(-63278l, sum);
		log.info("testReduce After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduceWithAdditionalMap() throws Throwable {
		log.info("testReduceWithAdditionalMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> intlist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.map(val -> Integer.parseInt(val[14])).reduce((a, b) -> a + b).map(val -> val + 10).collect(toexecute, new NumPartitions(3));
		int sum = 0;
		for (List<Integer> values : intlist) {
			for (Integer value : values) {
				log.info(value);
				sum += value;
			}
			log.info("");
		}
		assertEquals(-63268l, sum);
		log.info("testReduceWithAdditionalMap After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduceByKeyReduceByKey() throws Throwable {
		log.info("testReduceByKeyReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Integer> reducebykey1 = (MapPair<String, Integer>) datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);

		MapPair<String, Integer> reducebykey2 = (MapPair<String, Integer>) datastream2.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);
		List<List<Tuple2<Tuple2, Tuple2>>> tuplelist = reducebykey1.join(reducebykey2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).collect(toexecute, null);
		for (List<Tuple2<Tuple2, Tuple2>> values : tuplelist) {
			for (Tuple2<Tuple2, Tuple2> value : values) {
				log.info(value);
				assertEquals(value.v1.v1, value.v2.v1);
			}
			log.info("");
		}
		log.info("testReduceByKeyReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduceByKeyReduceByKeyPartitioned() throws Throwable {
		log.info("testReduceByKeyReduceByKeyPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Integer> reducebykey1 = (MapPair<String, Integer>) datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);

		MapPair<String, Integer> reducebykey2 = (MapPair<String, Integer>) datastream2.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);
		List<List<Tuple2<Tuple2, Tuple2>>> tuplelist = reducebykey1.join(reducebykey2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.collect(toexecute, new NumPartitions(3));
		for (List<Tuple2<Tuple2, Tuple2>> values : tuplelist) {
			for (Tuple2<Tuple2, Tuple2> value : values) {
				log.info(value);
				assertEquals(value.v1.v1, value.v2.v1);
			}
			log.info("");
		}
		log.info("testReduceByKeyReduceByKeyPartitioned After---------------------------------------");
	}


	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testReduceByKeyReduceByKeyPartitionedCoalesce() throws Throwable {
		log.info("testReduceByKeyReduceByKeyPartitionedCoalesce Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Integer> reducebykey1 = (MapPair<String, Integer>) datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);

		MapPair<String, Integer> reducebykey2 = (MapPair<String, Integer>) datastream2.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).reduceByKey((a, b) -> a + b);
		List<List<Tuple2<Tuple2, Tuple2>>> tuplelist = reducebykey1.join(reducebykey2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.coalesce(1, (tuple1, tuple2) -> Tuple.tuple(((Tuple2) tuple1).v1, ((Integer) ((Tuple2) tuple1).v2) + ((Integer) ((Tuple2) tuple2).v2)))
				.collect(toexecute, new NumPartitions(3));
		for (List<Tuple2<Tuple2, Tuple2>> values : tuplelist) {
			for (Tuple2<Tuple2, Tuple2> value : values) {
				log.info(value);
				assertEquals(value.v1.v1, value.v2.v1);
			}
		}
		log.info("testReduceByKeyReduceByKeyPartitionedCoalesce After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCoalesceCoalescePartitioned() throws Throwable {
		log.info("testCoalesceCoalescePartitioned Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Integer> coalesce1 = (MapPair<String, Integer>) datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).coalesce(1, (a, b) -> a + b);

		MapPair<String, Integer> coalesce2 = (MapPair<String, Integer>) datastream2.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).coalesce(1, (a, b) -> a + b);
		List<List<Tuple2<Tuple2, Tuple2>>> tuplelist = coalesce1.join(coalesce2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.collect(toexecute, new NumPartitions(3));
		for (List<Tuple2<Tuple2, Tuple2>> values : tuplelist) {
			for (Tuple2<Tuple2, Tuple2> value : values) {
				log.info(value);
				assertEquals(value.v1.v1, value.v2.v1);
			}
		}
		log.info("testCoalesceCoalescePartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCoalesceCoalesceDiffPartitioned() throws Throwable {
		log.info("testCoalesceCoalesceDiffPartitioned Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		MapPair<String, Integer> coalesce1 = (MapPair<String, Integer>) datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).coalesce(1, (a, b) -> a + b);

		MapPair<String, Integer> coalesce2 = (MapPair<String, Integer>) datastream2.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14]))).coalesce(1, (a, b) -> a + b);
		List<List<Tuple2<Tuple2, Tuple2>>> tuplelist = coalesce1.join(coalesce2, (tuple1, tuple2) -> true)
				.collect(toexecute, new NumPartitions(1));
		for (List<Tuple2<Tuple2, Tuple2>> values : tuplelist) {
			for (Tuple2<Tuple2, Tuple2> value : values) {
				log.info(value);
				assertEquals(value.v1.v1, value.v2.v1);
			}
		}
		log.info("testCoalesceCoalesceDiffPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked"})
	@Test
	public void testMappairCoalesce() throws Throwable {
		log.info("testMappairCoalesce Before---------------------------------------");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Integer>>> tuplelist = datastream1.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(val -> Tuple.tuple(val[8], Integer.parseInt(val[14])))
				.coalesce(1, (a, b) -> a + b).collect(toexecute, new NumPartitions(4));
		int sum = 0;
		for (List<Tuple2<String, Integer>> values : tuplelist) {
			for (Tuple2<String, Integer> value : values) {
				log.info(value);
				sum += value.v2;
			}
		}
		assertEquals(-63278l, sum);
		log.info("testMappairCoalesce After---------------------------------------");
	}
}
