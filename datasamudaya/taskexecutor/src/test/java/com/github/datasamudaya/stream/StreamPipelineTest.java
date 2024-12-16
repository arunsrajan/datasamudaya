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
import java.util.List;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.IntUnaryOperator;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVRecord;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Assert;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.functions.HashPartitioner;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineTest extends StreamPipelineBaseTestCommon {

	Logger log = LogManager.getLogger(StreamPipelineTest.class);

	boolean toexecute = true;

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCarsReduceByKey() throws Throwable {
		log.info("testCarsReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, cars, pipelineconfig);
		StreamPipeline<String[]> map = (StreamPipeline<String[]>) datastream.map(dat -> dat.split("\t"))
				.filter(dat -> !"make".equals(dat[0]));
		MapPair<String, Long> mappairweight = map
				.mapToPair(dat -> new Tuple2<String, Long>(dat[0], Long.parseLong(dat[8])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		MapPair<String, Long> mappaircount = map.mapToPair(dat -> new Tuple2<String, Long>(dat[0], 1l))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		List<List<Tuple2>> result = (List) mappairweight
				.join(mappaircount, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testCarsReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testCollectMultiMaps() throws Throwable {
		log.info("testCollectMultiMaps Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String[]> map = (StreamPipeline<String[]>) datastream.map(dat -> dat.split(","));

		List datacollected = (List) map.collect(toexecute, null);
		Assert.assertTrue(((List) datacollected.get(0)).size() == 46361);

		List<List<Tuple2<String, Long>>> count = (List) map.mapToPair(data -> Tuple.tuple(data[8], 1l))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sum = 0l;
		for (Tuple2 pair : count.get(0)) {
			sum += (Long) pair.v2;
		}
		log.info(sum);

		Assert.assertTrue(sum == 46361);
		log.info("testCollectMultiMaps After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFilterCollect() throws Throwable {
		log.info("testCsvStreamFilterCollect Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		List<List<CSVRecord>> result = (List) datastream.filter(record -> !"ArrDelay".equals(record.get(14))
				&& !"NA".equals(record.get(14)) && "4".equals(record.get(1)) && "13".equals(record.get("DayofMonth")))
				.collect(toexecute, null);
		for (CSVRecord csvrec : result.get(0)) {
			log.info(csvrec);
			assertEquals(true, "4".equals(csvrec.get(1)) && "13".equals(csvrec.get(2)));
		}

		log.info("testCsvStreamFilterCollect After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFlatMapLong() throws Throwable {
		log.info("testCsvStreamFlatMapLong Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		List<List<Long>> results = (List) datastream.flatMapToLong(csvrec -> Arrays
				.asList(!"NA".equals(csvrec.get("ArrDelay")) && !"ArrDelay".equals(csvrec.get("ArrDelay"))
						? Long.parseLong(csvrec.get("ArrDelay"))
						: 0)
				.stream()).collect(toexecute, null);
		long sum = results.stream().flatMap(str -> str.stream()).mapToLong(val -> val).sum();
		assertEquals(-63278, sum);
		log.info("testCsvStreamFlatMapLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamFlatMapPair() throws Throwable {
		log.info("testCsvStreamFlatMapPair Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		List<List<Tuple2>> results = (List) datastream
				.flatMapToTuple2(csvrec -> Arrays.asList(Tuple.tuple(csvrec.get("ArrDelay"), csvrec.get(2))))
				.collect(toexecute, null);
		double sum = results.stream().flatMap(str -> str.stream())
				.filter(t2 -> !t2.v1.equals("ArrDelay") && !t2.v1.equals("NA"))
				.mapToDouble(t2 -> Double.parseDouble((String) t2.v1)).sum();
		assertEquals(-63278.0, sum, 2);
		log.info("testCsvStreamFlatMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersection() throws Throwable {
		log.info("testCsvStreamIntersection Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter9 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 9l);
		StreamPipeline filter11 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 8l);
		List<List<CSVRecord>> csvrecords = (List) filter9.intersection(filter11).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "10".equals(csvrec.get(1)) || "11".equals(csvrec.get(1)) || "12".equals(csvrec.get(1)));

		}
		log.info("testCsvStreamIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersection10_8UnionGt10() throws Throwable {
		log.info("testCsvStreamIntersection10_8UnionGt10 Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter10 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 10l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 8l);
		StreamPipeline filtergt10 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 10l);
		List<List<CSVRecord>> csvrecords = (List) filter10.intersection(filter8).union(filtergt10).collect(toexecute,
				null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "11".equals(csvrec.get(1)) || "12".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamIntersection10_8UnionGt10 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamIntersectionIntersection() throws Throwable {
		log.info("testCsvStreamIntersectionIntersection Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter9 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 9l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 8l);
		StreamPipeline filter11 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 11l);
		List<List<CSVRecord>> csvrecords = (List) filter9.intersection(filter8).intersection(filter11)
				.collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "12".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamIntersectionIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamMapPair() throws Throwable {
		log.info("testCsvStreamMapPair Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		List<List<Tuple2>> results = (List) datastream
				.mapToPair(csvrec -> Tuple.tuple(csvrec.get("ArrDelay"), csvrec.get(2))).collect(toexecute, null);
		double sum = results.stream().flatMap(str -> str.stream())
				.filter(t2 -> !t2.v1.equals("ArrDelay") && !t2.v1.equals("NA"))
				.mapToDouble(t2 -> Double.parseDouble((String) t2.v1)).sum();
		assertEquals(-63278.0, sum, 2);
		log.info("testCsvStreamMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion() throws Throwable {
		log.info("testCsvStreamUnion Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter9 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 9l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 8l);
		List<List<CSVRecord>> csvrecords = (List) filter9.union(filter8).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "9".equals(csvrec.get(1)) || "8".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Intersectiongt8() throws Throwable {
		log.info("testCsvStreamUnion10_8Intersectiongt8 Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter10 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 10l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 8l);
		StreamPipeline filtergt8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) > 8l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).intersection(filtergt8).collect(toexecute,
				null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "10".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamUnion10_8Intersectiongt8 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Union8_10() throws Throwable {
		log.info("testCsvStreamUnion10_8Union8_10 Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter10 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 10l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 8l);
		StreamPipeline filter10_2 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 10l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).union(filter10_2).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "10".equals(csvrec.get(1)) || "8".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamUnion10_8Union8_10 After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCsvStreamUnion10_8Union8_12() throws Throwable {
		log.info("testCsvStreamUnion10_8Union8_12 Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		CsvStream<CSVRecord> csvStream = (CsvStream) datastream;
		StreamPipeline filter10 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 10l);
		StreamPipeline filter8 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 8l);
		StreamPipeline filter12 = csvStream
				.filter(record -> !"ArrDelay".equals(record.get(14)) && Long.parseLong(record.get(1)) == 12l);
		List<List<CSVRecord>> csvrecords = (List) filter10.union(filter8).union(filter12).collect(toexecute, null);
		for (CSVRecord csvrec : csvrecords.get(0)) {
			log.info(csvrec);
			assertEquals(true, "10".equals(csvrec.get(1)) || "8".equals(csvrec.get(1)) || "12".equals(csvrec.get(1)));
		}
		log.info("testCsvStreamUnion10_8Union8_12 After---------------------------------------");
	}

	@SuppressWarnings("rawtypes")
	@Test
	public void testFilter() throws Throwable {
		log.info("testFilter Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List data = (List) datastream.filter(dat -> "21".equals(dat.split(",")[2])).collect(toexecute, null);
		log.info(data);
		Assert.assertTrue(data.size() > 0);

		log.info("testFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFilterFilterFilter() throws Throwable {
		log.info("testFilterFilterFilter Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> filterfilterresult = (List) datastream.filter(dat -> "1".equals(dat.split(",")[2]))
				.filter(dat -> "1".equals(dat.split(",")[3])).filter(dat -> "1".equals(dat.split(",")[1]))
				.collect(true, null);
		assertTrue(filterfilterresult.get(0).size() > 0);
		log.info("testFilterFilterFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testFilterMultipleSortFields() throws Throwable {
		log.info("testFilterMultipleSortFields Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline map = (StreamPipeline) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]);
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
		});
		List<List<String>> values = (List) map.collect(toexecute, null);
		for (List<String> vals : values) {
			for (String valarr : vals) {
				log.info(valarr);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testFilterMultipleSortFields After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFilterPeek() throws Throwable {
		log.info("testFilterPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> values = (List) datastream.filter(value -> "10".equals(value.split(",")[1]))
				.peek(System.out::println).peek(valuearr -> System.out.println(valuearr)).collect(toexecute, null);
		assertTrue(values.get(0).size() > 0);
		log.info("testFilterPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testFilterSorted() throws Throwable {
		log.info("testFilterSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline map = (StreamPipeline) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]);
		}).sorted((val1, val2) -> {
			String[] vals1 = ((String) val1).split(",");
			String[] vals2 = ((String) val2).split(",");
			Long vall1 = Long.parseLong(vals1[2]);
			Long vall2 = Long.parseLong(vals2[2]);
			return vall1.compareTo(vall2);
		});
		List<List<String>> values = (List) map.collect(toexecute, null);
		for (List<String> vals : values) {
			for (String valarr : vals) {
				log.info(valarr);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testFilterSorted After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testFlatMap() throws Throwable {
		log.info("testFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List> words = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream()).collect(toexecute,
				null);
		for (Object word : words) {
			log.info(word);
		}
		assertTrue(words.get(0).size() > 0);
		log.info("testFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapFlatMapFlatMapFlatMap() throws Throwable {
		log.info("testFlatMapFlatMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Double>> longs = (List<List<Double>>) datastream.flatMap(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMap(value -> Arrays.asList(Double.valueOf(value)).stream())
				.flatMap(value -> Arrays.asList(value.longValue()).stream())
				.flatMap(value -> Arrays.asList(Double.valueOf(value)).stream()).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info("testFlatMapFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapFlatMapToDoubleFlatMapFlatMapToLong() throws Throwable {
		log.info("testFlatMapFlatMapToDoubleFlatMapFlatMapToLong Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMap(value -> {
			String values[] = value.split(",");
			return (Stream<Long>) Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value)).stream())
				.flatMap(value -> Arrays.asList(value.longValue()).stream())
				.flatMapToLong(value -> Arrays.asList(value).stream()).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapFlatMapToDoubleFlatMapFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapPeek() throws Throwable {
		log.info("testFlatMapPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> values = (List) datastream.flatMap(value -> Arrays.asList(value.split(",")).stream())
				.peek(System.out::println).collect(toexecute, null);
		assertTrue(values.get(0).size() > 0);
		log.info("testFlatMapPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToDouble() throws Throwable {
		log.info("testFlatMapToDouble Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToDouble(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Double
							.parseDouble(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToDoubleFlatMap() throws Throwable {
		log.info("testFlatMapToDoubleFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToDouble(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Double
							.parseDouble(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMap(value -> Arrays.asList(value).stream()).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToDoubleFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLong() throws Throwable {
		log.info("testFlatMapToLong Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(list -> list.longValue()).sum());
		log.info("testFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMap() throws Throwable {
		log.info("testFlatMapToLongFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMap(value -> Arrays.asList(value).stream()).collect(toexecute, null);

		assertEquals(-63278,
				longs.stream().flatMap(stream -> stream.stream()).mapToLong(value -> value.longValue()).sum());
		log.info("testFlatMapToLongFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapFlatMapFlatMap() throws Throwable {
		log.info("testFlatMapToLongFlatMapFlatMapFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMap(value -> Arrays.asList(value).stream()).flatMap(value -> Arrays.asList(value).stream())
				.flatMap(value -> Arrays.asList(value).stream()).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapToLongFlatMapFlatMapFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToDouble() throws Throwable {
		log.info("testFlatMapToLongFlatMapToDouble Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value)).stream()).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info("testFlatMapToLongFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble() throws Throwable {
		log.info(
				"testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Double>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMapToDouble(value -> Arrays.asList(Double.valueOf(value)).stream())
				.flatMapToLong(value -> Arrays.asList(value.longValue()).stream())
				.flatMapToDouble(value -> Arrays.asList(Double.valueOf(value)).stream()).collect(toexecute, null);

		assertEquals(-63278.0,
				longs.stream().flatMap(stream -> stream.stream()).mapToDouble(val -> val.doubleValue()).sum(), 2);
		log.info(
				"testFlatMapToLongFlatMapToDoubleFlatMapToLongFlatMapToDouble After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testFlatMapToLongFlatMapToLong() throws Throwable {
		log.info("testFlatMapToLongFlatMapToLong Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> longs = (List) datastream.flatMapToLong(value -> {
			String values[] = value.split(",");
			return Arrays
					.asList(Long
							.parseLong(!"NA".equals(values[14]) && !"ArrDelay".equals(values[14]) ? values[14] : "0"))
					.stream();
		}).flatMapToLong(value -> Arrays.asList(value).stream()).collect(toexecute, null);

		assertEquals(-63278, longs.stream().flatMap(stream -> stream.stream()).mapToLong(val -> val.longValue()).sum());
		log.info("testFlatMapToLongFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testGroupByKey() throws Throwable {
		log.info("testGroupByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List output = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).groupByKey().collect(toexecute, null);
		output.stream().forEach(log::info);
		log.info(output.size());

		log.info("testGroupByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersection() throws Throwable {
		log.info("testIntersection Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline dataveryverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> datas = (List) dataverysmall.intersection(dataveryverysmall).collect(true, null);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionFilterIntersection() throws Throwable {
		log.info("testIntersectionFilterIntersection Before---------------------------------------");
		StreamPipeline<String> datasmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> "10".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]));
		StreamPipeline<String> dataverysmall1987 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> "10".equals(dat.split(",")[1]) || "11".equals(dat.split(",")[1]));
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).collect(true, null);
		assertEquals(29, datas.get(0).size());
		log.info("testIntersectionFilterIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionIntersection() throws Throwable {
		log.info("testIntersectionIntersection Before---------------------------------------");
		StreamPipeline datasmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline dataverysmall1987 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).intersection(dataverysmall)
				.collect(true, null);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersectionIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionIntersectionReduceByKey() throws Throwable {
		log.info("testIntersectionIntersectionReduceByKey Before---------------------------------------");
		StreamPipeline<String> datasmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String> dataverysmall1987 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) datasmall.intersection(dataverysmall1987).intersection(dataverysmall)
				.map(dat -> dat.split(",")).filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);

		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testIntersectionIntersectionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionMapIntersection() throws Throwable {
		log.info("testIntersectionMapIntersection Before---------------------------------------");
		StreamPipeline<String> datasmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig)
				.map(dat -> dat);
		StreamPipeline<String> dataverysmall1987 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> datas = (List) datasmall.intersection(dataverysmall1987).collect(true, null);
		datas.get(0).stream().forEach(log::info);
		Assert.assertEquals(46361, datas.get(0).size());
		log.info("testIntersectionMapIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionMapPairIntersection() throws Throwable {
		log.info("testIntersectionMapPairIntersection Before---------------------------------------");
		StreamPipeline<String> mds = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig);
		MapPair<Tuple, Object> dataverysmall = mds.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		MapPair<Tuple, Object> datasmall = mds.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14])));
		List<List<Tuple2>> datas = (List) dataverysmall.intersection(datasmall).collect(true, null);
		assertEquals(25, datas.get(0).size());
		log.info("testIntersectionMapPairIntersection After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testIntersectionReduceByKey() throws Throwable {
		log.info("testIntersectionReduceByKey Before---------------------------------------");
		StreamPipeline<String> dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> dataveryverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) dataverysmall.intersection(dataveryverysmall).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testIntersectionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoin() throws Throwable {
		log.info("testJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> mappair2 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		MapPair<String, Long> mappair3 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2);

		MapPair<String, Long> mappair4 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2);

		MapPair<String, Long> mappair5 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2);

		List<List> result = (List) mappair1
				.join(mappair2, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair3, (tuple1, tuple2) -> (((Tuple2) ((Tuple2) tuple1).v1).v1).equals(((Tuple2) tuple2).v1))
				.join(mappair4,
						(tuple1, tuple2) -> ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1
								.equals(((Tuple2) tuple2).v1))
				.join(mappair5, (tuple1, tuple2) -> ((Tuple2) ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1).v1
						.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) pair.v1)).v1).v1).v1).v1,
						((Tuple2) pair.v2).v1);
			}
		}
		log.info("testJoin After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduce() throws Throwable {
		log.info("testJoinCommonMapMultipleReduce Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) airlinesamples
				.join(carriers, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testJoinCommonMapMultipleReduce After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinMapPair() throws Throwable {
		log.info("testJoinMapPair Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples1 = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MapPair<String, Long> airlinesamples2 = mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 - dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<String, Long> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) airlinesamples1.join(carriers).join(airlinesamples2).collect(toexecute, null);
		assertTrue(!result.isEmpty());
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				log.info(pair);
			}
		}

		log.info("testJoinMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testLeftJoinMapPair() throws Throwable {
		log.info("testLeftJoinMapPair Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples1 = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<String, Long> airlinesamples2 = mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 - dat2);

		MapPair<String, Long> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) carriers.leftJoin(airlinesamples1).leftJoin(airlinesamples2).collect(toexecute,
				null);
		assertTrue(!result.isEmpty());
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				log.info(pair);
			}
		}

		log.info("testLeftJoinMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testRightJoinMapPair() throws Throwable {
		log.info("testRightJoinMapPair Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples1 = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<String, Long> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.filter(line -> !"AQ".equals(line[0].substring(1, line[0].length() - 1)))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		MapPair<String, Long> carriers1 = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) carriers.rightJoin(airlinesamples1).leftJoin(carriers1).collect(toexecute, null);
		assertTrue(!result.isEmpty());
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				log.info(pair);
			}
		}

		log.info("testRightJoinMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduceLeftOuterJoin() throws Throwable {
		log.info("testJoinCommonMapMultipleReduceLeftOuterJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair airlinesamples = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair carriers = (MapPair) datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List> result = (List) airlinesamples
				.leftOuterjoin(carriers, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		int sumv1 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				if (((Tuple2) pair.v2) != null) {
					assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
				}
				sumv1 += (Long) ((Tuple2) pair.v1).v2;
			}
		}
		assertEquals(-63278l, sumv1);

		log.info("testJoinCommonMapMultipleReduceLeftOuterJoin After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testJoinCommonMapMultipleReduceRightOuterJoin() throws Throwable {
		log.info("testJoinCommonMapMultipleReduceRightOuterJoin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair airlinesample = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List<Tuple2>> result = (List) carriers
				.rightOuterjoin(airlinesample, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		int sumv1 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				if (((Tuple2) pair.v1) != null) {
					assertEquals(((Tuple2) pair.v1).v1, ((Tuple2) pair.v2).v1);
				}
				sumv1 += (Long) ((Tuple2) pair.v2).v2;
			}
		}
		assertEquals(-63278l, sumv1);

		log.info("testJoinCommonMapMultipleReduceRightOuterJoin After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testJoinPeek() throws Throwable {
		log.info("testJoinPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).peek(System.out::println)
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 + (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 + (Long) dat2).peek(System.out::println);

		MapPair mappair2 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((dat1, dat2) -> (Long) dat1 - (Long) dat2)
				.coalesce(1, (dat1, dat2) -> (Long) dat1 - (Long) dat2).peek(System.out::println);
		StreamPipeline<String> carriersstream = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);
		MapPair mappair3 = (MapPair) carriersstream.map(value -> value.split(","))
				.mapToPair(valuearr -> new Tuple2(valuearr[0].substring(1, valuearr[0].length() - 1),
						valuearr[1].substring(1, valuearr[1].length() - 1)))
				.peek(System.out::println);
		;
		List<List<Tuple2>> vals = (List) mappair1
				.join(mappair2, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair3, (tuple1, tuple2) -> ((Tuple2) ((Tuple2) tuple1).v1).v1.equals(((Tuple2) tuple2).v1))
				.peek(System.out::println).peek(System.out::println).collect(toexecute, null);
		for (Tuple2 pair : vals.get(0)) {
			log.info(pair.v1() + " " + pair.v2());
			assertTrue(((Tuple2) ((Tuple2) pair.v1).v1).v1.equals((((Tuple2) ((Tuple2) pair).v2).v1)));
		}
		log.info("testJoinPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapCsvStream() throws Throwable {
		log.info("testMapCsvStream Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig,
				airlineheader);
		List<List<Tuple2>> listreducebykey = (List) datastream.filter(
				dat -> dat != null && !"ArrDelay".equals(dat.get("ArrDelay")) && !"NA".equals(dat.get("ArrDelay")))
				.map(dat -> Tuple.tuple(dat.get(8), Long.parseLong(dat.get(14)))).collect(toexecute, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals("PS", tuple2.v1);
			assertEquals(true, (Long) tuple2.v2 >= -7);
		}

		log.info("testMapCsvStream After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapFilterSorted() throws Throwable {
		log.info("testMapFilterSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline map = (StreamPipeline) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14])).sorted((val1, val2) -> {
					Long vall1 = Long.parseLong(((String[]) val1)[2]);
					Long vall2 = Long.parseLong(((String[]) val2)[2]);
					return vall1.compareTo(vall2);
				});
		List<List<String[]>> values = (List) map.collect(toexecute, null);
		for (List<String[]> vals : values) {
			for (String[] valarr : vals) {
				log.info(valarr[0] + " " + valarr[1] + " " + valarr[2]);
			}
		}
		assertTrue(values.get(0).size() == 46360);
		log.info("testMapFilterSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappair() throws Throwable {
		log.info("testMapMappairFilter Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mappairresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !("ArrDelay".equals(dat[14]) || "NA".equals(dat[14])))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).collect(toexecute, null);
		List<List> arrDelayNA = (List<List>) datastream.map(dat -> dat.split(","))
				.filter(dat -> "ArrDelay".equals(dat[14]) || "NA".equals(dat[14])).collect(toexecute, null);
		long sum = 0;
		int totalValueCount = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
			totalValueCount += tuples.size();
		}
		int arrDelayNACount = 0;
		for (List arrDelay : arrDelayNA) {
			log.info(arrDelayNA);
			arrDelayNACount += arrDelay.size();
		}
		log.info(sum);
		log.info(arrDelayNACount);
		log.info(totalValueCount);
		assertTrue(-63278l == sum);
		assertEquals(46361 - arrDelayNACount, totalValueCount);
		log.info("testMapMappair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappairFilter() throws Throwable {
		log.info("testMapMappairFilter Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mappairresult = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.filter((Tuple2 pair) -> pair != null && (Long) ((Tuple2) pair).v2 > Long.MIN_VALUE)
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		log.info(sum);
		Assert.assertTrue(sum == -63278);

		log.info("testMapMappairFilter After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapMappairJoinPair() throws Throwable {
		log.info("testMapMappairJoinPair Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987, pipelineconfig);
		MapPair<String, Long> mappairfirst = datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);

		MapPair<String, Long> mappairsecond = datastream.map(dat -> dat.split(","))
				.filter(dat -> !"CRSElapsedTime".equals(dat[12]) && !"NA".equals(dat[12]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[12])))
				.reduceByKey((a, b) -> (Long) a + (Long) b).coalesce(1, (a, b) -> (Long) a + (Long) b);
		List<List<Tuple2>> mappairresult = (List) mappairfirst
				.join(mappairsecond, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2> tuples : mappairresult) {
			for (Tuple2 pair : tuples) {
				sum += (Long) ((Tuple2) pair.v1).v2;
			}
		}
		log.info(sum);
		assertTrue(6434 == sum);

		log.info("testMapMappairJoinPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapOfTupleOutput() throws Throwable {
		log.info("testMapOfTupleOutput Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.map(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).collect(toexecute, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals("PS", tuple2.v1);
			assertEquals(true, (Long) tuple2.v2 >= -7);
		}

		log.info("testMapOfTupleOutput After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMapPairCollect() throws Throwable {
		log.info("testMapPairCollect Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> result = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.map(dat -> Integer.parseInt(dat[14])).mapToPair(data -> new Tuple2(data, data))
				.collect(toexecute, null);
		int sumv1 = 0, sumv2 = 0;
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Integer) pair.v1;
				sumv2 += (Integer) pair.v2;
			}
		}
		assertEquals(sumv2, sumv1);
		assertEquals(-63278l, sumv1);
		assertEquals(-63278l, sumv2);

		log.info("testMapPairCollect After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked" })
	@Test
	public void testMapPeek() throws Throwable {
		log.info("testMapPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String[]>> values = (List<List<String[]>>) datastream.map(value -> value.split(","))
				.peek(System.out::println).peek(valuearr -> System.out.println(((String[]) valuearr)[1]))
				.collect(toexecute, null);
		assertTrue(values.get(0).size() == 46361);
		log.info("testMapPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPair() throws Throwable {
		log.info("testMapPair Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return Tuple.tuple(datarr[0], datarr[3]);
		}).collect(toexecute, null);
		mapPair6List.stream().flatMap(stream -> stream.stream())
				.forEach(tuple2 -> assertEquals(true,
						tuple2.v2.equals("1") || tuple2.v2.equals("2") || tuple2.v2.equals("3") || tuple2.v2.equals("4")
								|| tuple2.v2.equals("5") || tuple2.v2.equals("6") || tuple2.v2.equals("7")
								|| tuple2.v2.equals("DayOfWeek")));

		log.info("testMapPair After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFilter() throws Throwable {
		log.info("testMapPairFilter Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[2]);
		}).filter(tup -> {
			Tuple2 tup2 = (Tuple2) tup;
			return !tup2.v2.equals("DayofMonth") && Integer.parseInt((String) tup2.v2) >= 28;
		}).collect(toexecute, null);
		mapPair6List.stream().flatMap(stream -> stream.stream()).forEach(tup2 -> assertEquals(true,
				tup2.v2.equals("28") || tup2.v2.equals("29") || tup2.v2.equals("30") || tup2.v2.equals("31")));
		log.info("testMapPairFilter After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testMapPairFilterContains() throws Throwable {
		log.info("testMapPairFilterContains Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> datas = (List) datastream.mapToPair(data -> new Tuple2<String, String>(data, data))
				.filter(pair -> ((String) pair.v2).contains(",21,")).collect(toexecute, null);
		for (List<Tuple2> tuples : datas) {
			for (Tuple2 pair : tuples) {
				assertEquals(true, ((String) pair.v2).contains(",21,"));
			}
		}

		log.info("testMapPairFilterContains After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFlatMap() throws Throwable {
		log.info("testMapPairFlatMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mapPair2List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[3]);
		}).flatMap(tup2 -> Arrays.asList(tup2).stream()).collect(toexecute, null);
		mapPair2List.stream().flatMap(stream -> stream.stream()).forEach(System.out::println);
		log.info("testMapPairFlatMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairFlatMapToLong() throws Throwable {
		log.info("testMapPairFlatMapToLong Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Long>> mapPair6List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[14]);
		}).filter(tup2 -> !((Tuple2) tup2).v2.equals("NA") && !((Tuple2) tup2).v2.equals("ArrDelay"))
				.flatMapToLong(tup2 -> Arrays.asList(Long.parseLong((String) ((Tuple2) tup2).v2)))
				.collect(toexecute, null);
		assertEquals(-63278,
				mapPair6List.stream().flatMap(stream -> stream.stream()).mapToLong(list -> list.longValue()).sum());
		log.info("testMapPairFlatMapToLong After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairMap() throws Throwable {
		log.info("testMapPairMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> mapPair2List = (List) datastream.mapToPair(dat -> {
			String[] datarr = dat.split(",");
			return (Tuple2) Tuple.tuple(datarr[0], datarr[3]);
		}).map(tup2 -> {
			Tuple2 tuple = (Tuple2) tup2;
			return Tuple.tuple(tuple.v1, tuple.v2);
		}).collect(toexecute, null);
		mapPair2List.stream().flatMap(stream -> stream.stream())
				.forEach(tup2 -> assertEquals(true,
						tup2.v2.equals("1") || tup2.v2.equals("2") || tup2.v2.equals("3") || tup2.v2.equals("4")
								|| tup2.v2.equals("5") || tup2.v2.equals("6") || tup2.v2.equals("7")
								|| tup2.v2.equals("DayOfWeek")));
		log.info("testMapPairMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapPairPeek() throws Throwable {
		log.info("testMapPairPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> vals = (List) datastream.mapToPair(value -> {
			String[] values = value.split(",");
			return Tuple.tuple(values[8], values[14]);
		}).peek(tuple2 -> System.out.println(tuple2)).collect(toexecute, null);
		assertTrue(vals.get(0).size() == 46361);
		log.info("testMapPairPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapPairSorted() throws Throwable {
		log.info("testMapPairSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair mappair = (MapPair) datastream.filter(dat -> {
			String[] val = dat.split(",");
			return !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]);
		}).mapToPair(val -> {
			String[] valarr = val.split(",");
			return Tuple.tuple(valarr[8], Integer.parseInt(valarr[14]));
		}).sorted((val1, val2) -> {
			Tuple2 tup1 = (Tuple2) val1;
			Tuple2 tup2 = (Tuple2) val2;
			Integer vall1 = (Integer) tup1.v2;
			Integer vall2 = (Integer) tup2.v2;
			return vall1.compareTo(vall2);
		});
		List<List<Tuple2>> values = (List) mappair.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	public void testMapPairSortedPeek() throws Throwable {
		log.info("testMapPairSortedPeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
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
		List<List<Tuple2>> values = (List) mappair.peek(System.out::println).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> vals : values) {
			for (Tuple2 valarr : vals) {
				sum += (Integer) valarr.v2;
				log.info(valarr);
			}
		}
		assertTrue(sum == -63278);
		log.info("testMapPairSortedPeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMassiveDataPipelinePeek() throws Throwable {
		log.info("testMassiveDataPipelinePeek Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> vals = (List) datastream.peek(System.out::println).peek(System.out::println)
				.collect(toexecute, null);
		assertTrue(vals.get(0).size() == 46361);
		log.info("testMassiveDataPipelinePeek After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMultipleDAG() throws Throwable {
		log.info("testMultipleDAG Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		StreamPipeline<String> map1 = map.map(datas -> datas[0] + "" + datas[14]);
		StreamPipeline<String> map2 = map.map(datas -> datas[1] + "" + datas[14]);
		StreamPipeline<String> map3 = map.map(datas -> datas[2] + "" + datas[14]);
		StreamPipeline<String> map4 = map.map(datas -> datas[3] + datas[14]);
		List<List> lst1 = (List<List>) map1.collect(toexecute, null);
		List<List> lst2 = (List<List>) map2.collect(toexecute, null);
		List<List> lst3 = (List<List>) map3.collect(toexecute, null);
		List<List> lst4 = (List<List>) map4.collect(toexecute, null);
		assertEquals(lst1.get(0).size(), lst2.get(0).size());
		assertEquals(lst2.get(0).size(), lst3.get(0).size());
		assertEquals(lst3.get(0).size(), lst4.get(0).size());
		log.info("testMultipleDAG After---------------------------------------");
	}

	@SuppressWarnings({ "unused" })
	@Test
	public void testMultipleSplits() throws Throwable {
		log.info("testMultipleSplits Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String[]> map = datastream.map(dat -> dat.split(","));
		StreamPipeline<String[]> filt = map.filter(dat -> !"ArrDelay".equals(dat[14]));
		StreamPipeline<String> mapfilt = map.map(datas -> datas[0] + "" + datas[14])
				.filter(dat -> !"198723".equals(dat));
		StreamPipeline<String[]> filt1 = map.filter(dat -> !"ArrDelay".equals(dat[14]));
		StreamPipeline<String> mapfilt1 = map.map(datas -> datas[0] + "" + datas[14])
				.filter(dat -> !"198723".equals(dat));
		log.info(filt.collect(toexecute, null));
		log.info(mapfilt.collect(toexecute, null));

		log.info("testMultipleSplits After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testPartitioningEachFileReduceByKey() throws Throwable {
		log.info("testPartitioningEachFileReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinemultiplefilesfolder,
				pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, new NumPartitionsEachFile(5));
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-62663l, sum);
		log.info("testPartitioningEachFileReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testPartitioningReduceByKey() throws Throwable {
		log.info("testPartitioningReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, new NumPartitions(4));
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testPartitioningReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKey() throws Throwable {
		log.info("testReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2> tuples : redByKeyList) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		log.info(sum);
		assertTrue(-63278l == sum);

		log.info("testReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyBiCycleCrashSpeedLimit() throws Throwable {
		log.info("testReduceByKeyBiCycleCrashSpeedLimit Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, bicyclecrash, pipelineconfig);
		List<List> crashcounts = (List) datastream.map(dat -> dat.split(";")).filter(dat -> !"bike_sex".equals(dat[9]))
				.mapToPair(dat -> Tuple.tuple(dat[9] + "-" + dat[6] + "-" + dat[44], 1l))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object crashcount : crashcounts) {
			log.info(crashcount);
		}
		assertTrue(crashcounts.get(0).size() > 0);

		log.info("testReduceByKeyBiCycleCrashSpeedLimit After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierDayOfMonthArrDelay() throws Throwable {
		log.info("testReduceByKeyCarrierDayOfMonthArrDelay Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> carrierDayOfmonthArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[2], Long.parseLong(dat[14])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierDayOfmonthArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(-63278l == sumv1);
		log.info("testReduceByKeyCarrierDayOfMonthArrDelay After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierFlightNumDistance() throws Throwable {
		log.info("testReduceByKeyCarrierFlightNumDistance Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987, pipelineconfig);
		List<List> carrierFlightNumDistance = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"Distance".equals(dat[18]) && !"NA".equals(dat[18]))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[9], Long.parseLong(dat[18])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object carrierFlightNum : carrierFlightNumDistance) {
			log.info(carrierFlightNum);
		}
		assertTrue(carrierFlightNumDistance.get(0).size() > 0);

		log.info("testReduceByKeyCarrierFlightNumDistance After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierFlightNumOriginDistance() throws Throwable {
		log.info("testReduceByKeyCarrierFlightNumOriginDistance Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List> distancecount = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"Distance".equals(dat[18]) && !"NA".equals(dat[18]))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[9] + "-" + dat[16], Long.parseLong(dat[18])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		for (Object distance : distancecount) {
			log.info(distance);
		}
		assertTrue(distancecount.get(0).size() > 0);

		log.info("testReduceByKeyCarrierFlightNumOriginDistance After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCarrierMonthArrivalDelay() throws Throwable {
		log.info("testReduceByKeyCarrierMonthArrivalDelay Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987, pipelineconfig);
		List<List<Tuple2>> carrierMonthArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8] + "-" + dat[1], Long.parseLong(dat[14])))

				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);

		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierMonthArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(6434 == sumv1);

		log.info("testReduceByKeyCarrierMonthArrivalDelay After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyCsvStream() throws Throwable {
		log.info("testReduceByKeyCsvStream Before---------------------------------------");
		CsvStream<CSVRecord> datastream = StreamPipeline.newCsvStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig,
				airlineheader);
		List<List<Tuple2>> listreducebykey = (List) datastream.filter(
				dat -> dat != null && !"ArrDelay".equals(dat.get("ArrDelay")) && !"NA".equals(dat.get("ArrDelay")))
				.mapToPair(dat -> {
					return Tuple.tuple(dat.get(8), Long.parseLong(dat.get(14)));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		for (Tuple2 tuple2 : listreducebykey.get(0)) {
			log.info(tuple2);
			assertEquals(-63278l, ((Long) tuple2.v2).longValue());
		}

		log.info("testReduceByKeyCsvStream After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyPopulation() throws Throwable {
		log.info("testReduceByKeyPopulation Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, population, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"Abortion_rate".equals(dat[2])).mapToPair(dat -> {
					return new Tuple2<String, Double>(dat[1], Double.parseDouble(dat[2]));
				}).reduceByKey((pair1, pair2) -> (Double) pair1 + (Double) pair2)
				.coalesce(1, (pair1, pair2) -> (Double) pair1 + (Double) pair2).collect(toexecute, null);
		double sum = 0;
		for (Tuple2 pair : listreducebykey.get(0)) {
			log.info(pair.v1 + " " + pair.v2);
			sum += (double) pair.v2;
		}
		log.info(sum);

		log.info("testReduceByKeyPopulation After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testReduceByKeyVerySmall() throws Throwable {
		log.info("testReduceByKeyVerySmall Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987, pipelineconfig);
		List<List<Tuple2>> carrierArrDelay = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		long sumv1 = 0;
		for (List<Tuple2> tuples : carrierArrDelay) {
			for (Tuple2 pair : tuples) {
				sumv1 += (Long) pair.v2;
			}
		}
		log.info(sumv1);
		assertTrue(6434 == sumv1);

		log.info("testReduceByKeyVerySmall After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSample() throws Throwable {
		log.info("testSample Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.sample(46361).map(datas -> datas.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testSample After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testSampleSample() throws Throwable {
		log.info("testSampleSample Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.sample(46361).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14]))).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		int sum = 0;
		for (List<Tuple2> tuples : listreducebykey) {
			for (Tuple2 pair : tuples) {
				sum += (Long) pair.v2;
			}
		}
		assertEquals(-63278l, sum);

		log.info("testSampleSample After---------------------------------------");
	}

	@SuppressWarnings({ "rawtypes", "unchecked" })
	@Test
	public void testSingleMappairMultipleReduceByKey() throws Throwable {
		log.info("testSingleMappairMultipleReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> mappair2 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MapPair<String, Long> mappair3 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2).coalesce(1,
				(dat1, dat2) -> dat1 - dat2);

		MapPair<String, Long> mappair4 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat1 - dat2 + dat1).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		MapPair<String, Long> mappair5 = (MapPair) mappair1.reduceByKey((dat1, dat2) -> dat2 - dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		List<List> result = (List) mappair2
				.join(mappair3, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.join(mappair4, (tuple1, tuple2) -> (((Tuple2) ((Tuple2) tuple1).v1).v1).equals(((Tuple2) tuple2).v1))
				.join(mappair5, (tuple1, tuple2) -> ((Tuple2) (((Tuple2) ((Tuple2) tuple1).v1).v1)).v1
						.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples : result) {
			for (Tuple2 pair : tuples) {
				assertEquals(((Tuple2) ((Tuple2) ((Tuple2) ((Tuple2) pair.v1)).v1).v1).v1, ((Tuple2) pair.v2).v1);
			}
		}

		log.info("testSingleMappairMultipleReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testStreamSampleSampleSample() throws Throwable {
		log.info("testStreamSampleSampleSample Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2>> listreducebykey = (List) datastream.sample(10000).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).sample(1000)
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(toexecute, null);
		int sum = 0;
		for (Tuple2 pair : listreducebykey.get(0)) {
			log.info(pair.v1 + " " + pair.v2);
			sum += (Long) pair.v2;
		}
		log.info(sum);
		assertEquals(-1308, sum);
		log.info("testStreamSampleSampleSample After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnion() throws Throwable {
		log.info("testUnion Before---------------------------------------");
		StreamPipeline dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline dataveryverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> datas = (List) dataverysmall.union(dataveryverysmall).collect(true, null);
		assertEquals(46361, datas.get(0).size());
		log.info("testUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionFilterUnion() throws Throwable {
		log.info("testUnionFilterUnion Before---------------------------------------");
		StreamPipeline<String> dataverysmall = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> "10".equals(dat.split(",")[2]) || "11".equals(dat.split(",")[2]));
		StreamPipeline<String> dataveryverysmall1989 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig)
				.filter(dat -> "12".equals(dat.split(",")[2]) || "13".equals(dat.split(",")[2]));
		List<List<String>> datas = (List) dataverysmall.union(dataveryverysmall1989).collect(true, null);
		Assert.assertEquals(4, datas.get(0).size());
		for (String data : datas.get(0)) {
			String value = data.split(",")[2];
			Assert.assertEquals(true,
					"10".equals(value) || "11".equals(value) || "12".equals(value) || "13".equals(value));
		}
		log.info("testUnionFilterUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionMapPairUnion() throws Throwable {
		log.info("testUnionFilterMapPairUnion Before---------------------------------------");
		MapPair<Tuple, Object> dataverysmall = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		MapPair<Tuple, Object> dataveryverysmall1989 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return (Tuple2) Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				});
		List<List<Tuple>> datas = (List) dataverysmall.union(dataveryverysmall1989).collect(true, null);
		Assert.assertEquals(25, datas.get(0).size());
		log.info("testUnionFilterMapPairUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionMapUnion() throws Throwable {
		log.info("testUnionMapUnion Before---------------------------------------");
		StreamPipeline<String> unionstream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinepairjoin, pipelineconfig).map(dat -> dat);
		StreamPipeline<String> unionstream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinepairjoin,
				pipelineconfig);
		List<List<String>> datas = (List) unionstream1.union(unionstream2).collect(true, null);
		Assert.assertEquals(30, datas.get(0).size());
		log.info("testUnionMapUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionReduceByKey() throws Throwable {
		log.info("testUnionReduceByKey Before---------------------------------------");
		StreamPipeline<String> dataverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> dataveryverysmall = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = (List) dataverysmall.union(dataveryverysmall).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testUnionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionUnion() throws Throwable {
		log.info("testUnionUnion Before---------------------------------------");
		StreamPipeline unionstream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline unionstream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline unionstream3 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<String>> datas = (List) unionstream1.union(unionstream2).union(unionstream3).collect(true, null);
		assertEquals(46361, datas.get(0).size());
		log.info("testUnionUnion After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testUnionUnionReduceByKey() throws Throwable {
		log.info("testUnionUnionReduceByKey Before---------------------------------------");
		StreamPipeline<String> unionstream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String> unionstream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String> unionstream3 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple>> datas = (List) unionstream1.union(unionstream2).union(unionstream3).map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14])).mapToPair(dat -> {
					return Tuple.tuple(dat[8], Long.parseLong(dat[14]));
				}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2)
				.collect(true, null);
		assertEquals(-63278l, ((Tuple2) datas.get(0).get(0)).v2);
		log.info("testUnionUnionReduceByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCount() throws Throwable {
		log.info("testWordCount Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute, null);
		wordscount.stream().forEach(log::info);

		log.info("testWordCount After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountCountAndWordSorted() throws Throwable {
		log.info("testWordCountCountAndWordSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					int compres = ((Long) tup1.v2).compareTo(((Long) tup2.v2));
					if (compres == 0) {
						return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
					}
					return compres;
				}).collect(toexecute, null);
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);

		log.info("testWordCountCountAndWordSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountCountSorted() throws Throwable {
		log.info("testWordCountCountSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					return ((Long) tup1.v2).compareTo(((Long) tup2.v2));
				}).collect(toexecute, null);
		wordscount.stream().flatMap(stream -> stream.stream()).forEach(log::info);

		log.info("testWordCountCountSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountFlatMapToTuple() throws Throwable {
		log.info("testWordCountFlatMapToTuple Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List wordscount = (List) datastream.flatMapToTuple2(str -> {
			String[] strarr = str.split(" ");
			List<Tuple2<String, Long>> pairs = (List) Arrays.asList(strarr).parallelStream().map(stra -> {
				return (Tuple2<String, Long>) Tuple.tuple(stra, 1l);
			}).collect(Collectors.toCollection(Vector::new));
			return pairs;
		}).reduceByKey((a, b) -> a + b).coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).collect(toexecute,
				null);
		wordscount.stream().forEach(log::info);

		log.info("testWordCountFlatMapToTuple After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testWordCountWordSorted() throws Throwable {
		log.info("testWordCountWordSorted Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream())
				.mapToPair(str -> Tuple.tuple(str.trim().replace(" ", ""), (Long) 1l)).reduceByKey((a, b) -> a + b)
				.coalesce(1, (pair1, pair2) -> (Long) pair1 + (Long) pair2).sorted((val1, val2) -> {
					Tuple2 tup1 = (Tuple2) val1;
					Tuple2 tup2 = (Tuple2) val2;
					return ((String) tup1.v1).compareToIgnoreCase(((String) tup2.v1));
				}).collect(toexecute, null);
		int numberofwords = 0;
		int numberofdistinctwords = 0;
		for (List<Tuple2> vals : wordscount) {
			numberofdistinctwords += vals.size();
			for (Tuple2 valarr : vals) {
				numberofwords += (Long) valarr.v2;
				log.info(valarr);
			}
		}
		assertEquals(201, numberofwords);
		assertEquals(121, numberofdistinctwords);
		log.info("testWordCountWordSorted After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testDistinct() throws Throwable {
		log.info("testDistinct Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		List<List> wordscount = (List) datastream.flatMap(str -> Arrays.asList(str.split(" ")).stream()).distinct()
				.collect(toexecute, null);
		for (List<String> vals : wordscount) {
			for (String distinctwords : vals)
				log.info(distinctwords);
		}
		assertEquals(121, wordscount.get(0).size());
		log.info("testDistinct After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToInt() throws Throwable {
		log.info("testMapToInt Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToInt(str -> Integer.parseInt(str[14])).<AtomicInteger>collect(toexecute, () -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(-63278, intvalues.get(0).get(0).get());
		log.info("testMapToInt After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToIntDistinct() throws Throwable {
		log.info("testMapToIntDistinct Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToInt(str -> Integer.parseInt(str[14])).distinct().<AtomicInteger>collect(toexecute,
						() -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(26962, intvalues.get(0).get(0).get());
		log.info("testMapToIntDistinct After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testMapToIntMap() throws Throwable {
		log.info("testMapToIntMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<AtomicInteger>> intvalues = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToInt(str -> Integer.parseInt(str[14])).map(new IntUnaryOperator() {

					@Override
					public int applyAsInt(int operand) {
						return operand + 100;
					}

				}).<AtomicInteger>collect(toexecute, () -> new AtomicInteger(),
						(AtomicInteger a, int b) -> ((AtomicInteger) a).set(((AtomicInteger) a).get() + b),
						(AtomicInteger a, AtomicInteger b) -> a.set(a.get() + b.get()));
		for (List<AtomicInteger> vals : intvalues) {
			for (AtomicInteger intvalue : vals)
				log.info(intvalue);
		}
		assertEquals(4532422, intvalues.get(0).get(0).get());
		log.info("testMapToIntMap After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testCountByKey() throws Throwable {
		log.info("testCountByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14])).mapToPair(str -> Tuple.tuple(str[1], str[14])).countByKey()
				.collect(toexecute, new NumPartitions(3));
		long sum = 0;
		for (List<Tuple> tuples : tupleslist) {
			for (Tuple tuple : tuples) {
				log.info(tuple);
				sum += (long) ((Tuple2) tuple).v2;
			}
			log.info("");
		}
		assertEquals(46360l, sum);
		log.info("testCountByKey After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testHashPartitioner() throws Throwable {
		log.info("testHashPartitioner Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2<Integer, List<Tuple2>>>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14])).mapToPair(str -> Tuple.tuple(str[1], str[14]))
				.partition(new HashPartitioner(3)).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<Integer, List<Tuple2>>> tuples : tupleslist) {
			for (Tuple2<Integer, List<Tuple2>> tuplespartitioned : tuples) {
				sum += tuplespartitioned.v2.size();
				log.info(tuplespartitioned.v1);
			}
		}
		assertEquals(46360l, sum);
		log.info("testHashPartitioner After---------------------------------------");
	}

	@SuppressWarnings({ "unchecked", "rawtypes" })
	@Test
	public void testHashPartitionerReduceByKey() throws Throwable {
		log.info("testHashPartitionerReduceByKey Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		List<List<Tuple2<String, Integer>>> tupleslist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]))
				.mapToPair(str -> new Tuple2<String, Integer>(str[1], Integer.parseInt(str[14])))
				.partition(new HashPartitioner(3)).flatMap(tuples -> tuples.v2().stream()).reduceByKey((a, b) -> a + b)
				.collect(toexecute, null);
		int sum = 0;
		assertEquals(1, tupleslist.size());
		for (List<Tuple2<String, Integer>> tuples : tupleslist) {
			for (Tuple2<String, Integer> tuple2 : tuples) {
				log.info(tuple2);
				sum += tuple2.v2();
			}
			log.info("");
		}
		assertEquals(-63278l, sum);
		log.info("testHashPartitionerReduceByKey After---------------------------------------");
	}

}
