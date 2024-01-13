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

import java.net.URI;
import java.util.Arrays;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;

public class StreamPipelineTransformationsSaveToFileTest extends StreamPipelineBaseTestCommon {

	static String modeLocal = "";
	static String modeMesos = "";
	static String modeYarn = "";
	static String modeJgroups = "";
	boolean toexecute = true;

	@BeforeClass
	public static void getSetExecutorMode() {
		modeLocal = pipelineconfig.getLocal();
		modeMesos = pipelineconfig.getMesos();
		modeYarn = pipelineconfig.getYarn();
		modeJgroups = pipelineconfig.getJgroups();
		pipelineconfig.setLocal("true");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
	}

	@AfterClass
	public static void setExecutorMode() {
		pipelineconfig.setLocal(modeLocal);
		pipelineconfig.setMesos(modeMesos);
		pipelineconfig.setYarn(modeYarn);
		pipelineconfig.setJgroups(modeJgroups);
	}


	@Test
	public void testPipelineMap() throws Exception {
		StreamPipeline<String> pipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String[]> map = pipeline.map(dat -> dat.split(","));
		map.saveAsTextFile(new URI(hdfsfilepath),
				"/transformation/Trans-" + System.currentTimeMillis());
	}


	@Test
	public void testPipelineFlatMap() throws Exception {
		StreamPipeline<String> pipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount,
				pipelineconfig);
		StreamPipeline<String> flatmap = pipeline.flatMap(dat -> Arrays.asList(dat.split(" ")).stream());
		flatmap.saveAsTextFile(new URI(hdfsfilepath),
				"/transformation/Trans-" + System.currentTimeMillis());

	}

	@Test
	public void testMap() throws Exception {
		log.info("testMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		datastream.map(val -> val.split(DataSamudayaConstants.COMMA)[0]).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMap After---------------------------------------");
	}

	@Test
	public void testFilter() throws Exception {
		log.info("testFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.filter(val -> "2007".equals(val.split(DataSamudayaConstants.COMMA)[0])
				|| "Year".equalsIgnoreCase(val.split(DataSamudayaConstants.COMMA)[0])).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testFilter After---------------------------------------");
	}

	@Test
	public void testflatMap() throws Exception {
		log.info("testflatMap Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.flatMap(val -> Arrays.asList(val.split(DataSamudayaConstants.COMMA)).stream())
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testflatMap After---------------------------------------");
	}

	@Test
	public void testflatMapToDouble() throws Exception {
		log.info("flatMapToDouble Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.flatMapToDouble(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			try {
				return Arrays.asList(Double.parseDouble(values[0])).stream();
			} catch (Exception ex) {
				return Arrays.asList(0d).stream();
			}
		}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("flatMapToDouble After---------------------------------------");
	}

	@Test
	public void testflatMapToLong() throws Exception {
		log.info("testflatMapToLong Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.flatMapToLong(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			try {
				return Arrays.asList(Long.parseLong(values[0])).stream();
			} catch (Exception ex) {
				return Arrays.asList(0l).stream();
			}
		}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testflatMapToLong After---------------------------------------");
	}

	@Test
	public void testflatMapToTuple2() throws Exception {
		log.info("testflatMapToTuple2 Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.flatMapToTuple2(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			return Arrays.asList((Tuple2<String, String>) Tuple.tuple(values[14], values[8]));
		}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testflatMapToTuple2 After---------------------------------------");
	}

	@Test
	public void testflatMapToTuple() throws Exception {
		log.info("testflatMapToTuple Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.flatMapToTuple(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			return Arrays.asList(Tuple.tuple(values[0], values[1], values[14], values[8]));
		}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testflatMapToTuple After---------------------------------------");
	}

	@Test
	public void testKeyByFunction() throws Exception {
		log.info("testKeyByFunction Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream.keyBy(val -> val.split(DataSamudayaConstants.COMMA)[0]).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testKeyByFunction After---------------------------------------");
	}

	@Test
	public void testLeftOuterJoin() throws Exception {
		log.info("testLeftOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.leftOuterjoin(datastream2, (val1, val2) -> val1.equals(val2))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoin After---------------------------------------");
	}

	@Test
	public void testLeftOuterJoinFilter() throws Exception {
		log.info("testLeftOuterJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig)
				.filter(val -> "1".equals(val.split(DataSamudayaConstants.COMMA)[1]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.leftOuterjoin(datastream2, (val1, val2) -> val1.equals(val2))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoinFilter After---------------------------------------");
	}

	@Test
	public void testJoin() throws Exception {
		log.info("testJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.join(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute,
				null);
		pipelineconfig.setLocal(local);
		log.info("testJoin After---------------------------------------");
	}

	@Test
	public void testJoinFilter() throws Exception {
		log.info("testJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.join(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute,
				null);
		pipelineconfig.setLocal(local);
		log.info("testJoinFilter After---------------------------------------");
	}

	@Test
	public void testRightOuterJoin() throws Exception {
		log.info("testRightOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream2.rightOuterjoin(datastream1, (val1, val2) -> val1.equals(val2))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoin After---------------------------------------");
	}

	@Test
	public void testRightOuterJoinFilterReverse() throws Exception {
		log.info("testRightOuterJoinFilterReverse Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream2.rightOuterjoin(datastream1, (val1, val2) -> val1.equals(val2))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinFilterReverse After---------------------------------------");
	}

	@Test
	public void testRightOuterJoinFilter() throws Exception {
		log.info("testRightOuterJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.rightOuterjoin(datastream2, (val1, val2) -> val1.equals(val2))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinFilter After---------------------------------------");
	}

	@Test
	public void testUnion() throws Exception {
		log.info("testUnion Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.union(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testUnion After---------------------------------------");
	}

	@Test
	public void testUnionFilter() throws Exception {
		log.info("testUnionFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		datastream1.union(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testUnionFilter After---------------------------------------");
	}

	@Test
	public void testUnionFilterFilter() throws Exception {
		log.info("testUnionFilterFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airline1987, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		datastream1.union(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testUnionFilterFilter After---------------------------------------");
	}

	@Test
	public void testIntersection() throws Exception {
		log.info("testIntersection Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.intersection(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testIntersection After---------------------------------------");
	}

	@Test
	public void testIntersectionFilter() throws Exception {
		log.info("testIntersectionFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		datastream1.intersection(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testIntersectionFilter After---------------------------------------");
	}

	@Test
	public void testIntersectionFilterFilter() throws Exception {
		log.info("testIntersectionFilterFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesamplejoin, pipelineconfig)
				.filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
						|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		datastream1.intersection(datastream2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testIntersectionFilterFilter After---------------------------------------");
	}

	@Test
	public void testPeek() throws Exception {
		log.info("testPeek Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.peek(System.out::println).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testPeek After---------------------------------------");
	}

	@Test
	public void testFilterSorted() throws Exception {
		log.info("testFilterSorted Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1.filter(value -> !"DayofMonth".equals(value.split(DataSamudayaConstants.COMMA)[2]))
				.sorted((val1, val2) -> {
					return Integer.valueOf(val1.split(DataSamudayaConstants.COMMA)[2])
							.compareTo(Integer.valueOf(val2.split(DataSamudayaConstants.COMMA)[2]));
				}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testFilterSorted After---------------------------------------");
	}

	@Test
	public void testMapPair() throws Exception {
		log.info("testMapPair Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		datastream1
				.mapToPair(
						value -> Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[8], value.split(DataSamudayaConstants.COMMA)[14]))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPair After---------------------------------------");
	}

	@Test
	public void testMapPairFilter() throws Exception {
		log.info("testMapPairFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1
				.mapToPair(
						value -> Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.filter(tup -> tup.v1.equals("1")).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairFilter After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMap() throws Exception {
		log.info("testMapPairFlatMap Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1
				.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
						value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMap(tuple -> Arrays.asList(tuple, tuple).stream()).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMap After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapToLong() throws Exception {
		log.info("testMapPairFlatMapToLong Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMapToLong(tuple -> {
					String val = tuple.v1;
					if (!"Month".equals(val)) {
						return Arrays.asList(Tuple.tuple(Long.parseLong(val), Long.parseLong(val)));
					}
					return Arrays.asList(Tuple.tuple(0l, 0l));
				}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToLong After---------------------------------------");
	}

	@Test
	public void testMapPairFlatMapToDouble() throws Exception {
		log.info("testMapPairFlatMapToDouble Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMapToDouble(tuple -> {
					String val = tuple.v1;
					if (!"Month".equals(val)) {
						return Arrays.asList(Tuple.tuple(Double.parseDouble(val), Double.parseDouble(val))).stream();
					}
					return Arrays.asList(Tuple.tuple(0d, 0d)).stream();
				}).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToDouble After---------------------------------------");
	}

	@Test
	public void testMapPairCoalesce() throws Exception {
		log.info("testMapPairCoalesce Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).coalesce(1, (a, b) -> a + b).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairCoalesce After---------------------------------------");
	}

	@Test
	public void testMapPairReduceByKey() throws Exception {
		log.info("testMapPairReduceByKey Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).reduceByKey((a, b) -> a + b).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairReduceByKey After---------------------------------------");
	}

	@Test
	public void testMapPairSample() throws Exception {
		log.info("testMapPairSample Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).sample(100).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairSample After---------------------------------------");
	}

	@Test
	public void testMapPairPeek() throws Exception {
		log.info("testMapPairPeek Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).peek(System.out::println).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testMapPairPeek After---------------------------------------");
	}

	@Test
	public void testMapPairFoldLeft() throws Exception {
		log.info("testMapPairFoldLeft Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).foldLeft(0l, (a, b) -> a + b, 1, (a, b) -> a + b).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldLeft After---------------------------------------");
	}

	@Test
	public void testMapPairFoldRight() throws Exception {
		log.info("testMapPairFoldRight Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).foldRight(0l, (a, b) -> a + b, 1, (a, b) -> a + b).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());

		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldRight After---------------------------------------");
	}

	@Test
	public void testMapPairGroupByKey() throws Exception {
		log.info("testMapPairGroupByKey Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).groupByKey().saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairGroupByKey After---------------------------------------");
	}

	@Test
	public void testMapPairJoin() throws Exception {
		log.info("testMapPairJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		MapPair<String, String> mtsl2 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		mtsl1
				.join(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairJoin After---------------------------------------");
	}

	@Test
	public void testMapPairLeftOuterJoin() throws Exception {
		log.info("testMapPairLeftOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		MapPair<String, String> mtsl2 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		mtsl1
				.leftOuterjoin(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairLeftOuterJoin After---------------------------------------");
	}

	@Test
	public void testMapPairRightOuterJoin() throws Exception {
		log.info("testMapPairRightOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		MapPair<String, String> mtsl2 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		mtsl1
				.rightOuterjoin(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairRightOuterJoin After---------------------------------------");
	}

	@Test
	public void testMapPairUnion() throws Exception {
		log.info("testMapPairUnion Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		MapPair<String, String> mtsl2 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		mtsl1.union(mtsl2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairUnion After---------------------------------------");
	}

	@Test
	public void testMapPairIntersection() throws Exception {
		log.info("testMapPairIntersection Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		MapPair<String, String> mtsl2 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
				.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
		mtsl1.intersection(mtsl2).saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairIntersection After---------------------------------------");
	}

	@Test
	public void testMapPairSorted() throws Exception {
		log.info("testMapPairSorted Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		datastream1
				.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[0],
						value.split(DataSamudayaConstants.COMMA)[1]))
				.sorted((tuple1, tuple2) -> ((String) tuple1.v1).compareTo((String) tuple2.v1))
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testMapPairSorted After---------------------------------------");
	}

	@Test
	public void testMapFilterMapPairCogroup() throws Exception {
		log.info("testFilterMapPairCogroup Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String[]> datastream1 = StreamPipeline
				.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig).map(val -> val.split(","))
				.filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]));
		MapPair<String, Long> data1 = datastream1
				.mapToPair(value -> (Tuple2<String, Long>) Tuple.tuple(value[8], Long.parseLong(value[14])));

		MapPair<String, Long> data2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig)
				.map(val -> val.split(",")).filter(val -> !"ArrDelay".equals(val[14]) && !"NA".equals(val[14]))
				.mapToPair(value -> (Tuple2<String, Long>) Tuple.tuple(value[8], Long.parseLong(value[14])));
		data1.cogroup(data2)
				.saveAsTextFile(new URI(hdfsfilepath), "/transformation/Trans-" + System.currentTimeMillis());
		pipelineconfig.setLocal(local);
		log.info("testFilterMapPairCogroup After---------------------------------------");
	}

}
