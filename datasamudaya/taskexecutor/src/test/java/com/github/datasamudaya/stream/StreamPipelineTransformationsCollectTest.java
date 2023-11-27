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

import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.io.LineNumberReader;
import java.util.Arrays;
import java.util.List;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;

public class StreamPipelineTransformationsCollectTest extends StreamPipelineBaseTestCommon {

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
		modeJgroups =  pipelineconfig.getJgroups();
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

	@SuppressWarnings("unchecked")
	@Test
	public void testPipelineMap() throws Exception {
		StreamPipeline<String> pipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample, pipelineconfig);
		StreamPipeline<String[]> map = pipeline.map(dat -> dat.split(","));
		List<String[]> mapcollect = map.collect(true, null);
		assertTrue(mapcollect.size() > 0);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testPipelineFlatMap() throws Exception {
		StreamPipeline<String> pipeline = StreamPipeline.newStreamHDFS(hdfsfilepath, wordcount, pipelineconfig);
		StreamPipeline<String> flatmap = pipeline.flatMap(dat -> Arrays.asList(dat.split(" ")).stream());
		List<String> flatmapcollect = flatmap.collect(true, null);
		assertTrue(flatmapcollect.size() > 0);
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMap() throws Exception {
		log.info("testMap Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream.map(val -> val.split(DataSamudayaConstants.COMMA)[0]).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testMap After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testFilter() throws Exception {
		log.info("testFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream.filter(val -> "2007".equals(val.split(DataSamudayaConstants.COMMA)[0]) || "Year".equalsIgnoreCase(val.split(DataSamudayaConstants.COMMA)[0])).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testflatMap() throws Exception {
		log.info("testflatMap Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream.flatMap(val -> Arrays.asList(val.split(DataSamudayaConstants.COMMA)).stream()).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count * 29, sum);
		pipelineconfig.setLocal(local);
		log.info("testflatMap After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testflatMapToDouble() throws Exception {
		log.info("flatMapToDouble Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<Double>> data = datastream.flatMapToDouble(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			try {
				return Arrays.asList(Double.parseDouble(values[0])).stream();
			}
			catch (Exception ex) {
				return Arrays.asList(0d).stream();
			}
		}).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("flatMapToDouble After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testflatMapToLong() throws Exception {
		log.info("testflatMapToLong Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<Long>> data = datastream.flatMapToLong(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			try {
				return Arrays.asList(Long.parseLong(values[0])).stream();
			}
			catch (Exception ex) {
				return Arrays.asList(0l).stream();
			}
		}).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testflatMapToLong After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testflatMapToTuple2() throws Exception {
		log.info("testflatMapToTuple2 Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<Tuple2<String, String>>> data = datastream.flatMapToTuple2(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			return Arrays.asList((Tuple2<String, String>) Tuple.tuple(values[14], values[8]));
		}).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testflatMapToTuple2 After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testflatMapToTuple() throws Exception {
		log.info("testflatMapToTuple Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<Tuple>> data = datastream.flatMapToTuple(val -> {
			String[] values = val.split(DataSamudayaConstants.COMMA);
			return Arrays.asList(Tuple.tuple(values[0], values[1], values[14], values[8]));
		}).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testflatMapToTuple After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testKeyByFunction() throws Exception {
		log.info("testKeyByFunction Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<Tuple>> data = datastream.keyBy(val -> val.split(DataSamudayaConstants.COMMA)[0]).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testKeyByFunction After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testLeftOuterJoin() throws Exception {
		log.info("testLeftOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesample.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream1.leftOuterjoin(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoin After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testLeftOuterJoinFilter() throws Exception {
		log.info("testLeftOuterJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig).filter(val -> "1".equals(val.split(DataSamudayaConstants.COMMA)[1]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<String>> data = datastream1.leftOuterjoin(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(3875, sum);
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoinFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testJoin() throws Exception {
		log.info("testJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream1.join(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testJoin After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testJoinFilter() throws Exception {
		log.info("testJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> data = datastream1.join(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(3, sum);
		pipelineconfig.setLocal(local);
		log.info("testJoinFilter After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testRightOuterJoin() throws Exception {
		log.info("testRightOuterJoin Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		LineNumberReader linereader = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader.lines().count();
		linereader.close();
		List<List<String>> data = datastream2.rightOuterjoin(datastream1, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoin After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testRightOuterJoinFilterReverse() throws Exception {
		log.info("testRightOuterJoinFilterReverse Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).map(val -> val);
		List<List<String>> data = datastream2.rightOuterjoin(datastream1, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(3, sum);
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinFilterReverse After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testRightOuterJoinFilter() throws Exception {
		log.info("testRightOuterJoinFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> data = datastream1.rightOuterjoin(datastream2, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(20, sum);
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinFilter After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testUnion() throws Exception {
		log.info("testUnion Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader1 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airline1987.csv")));
		long count = linereader1.lines().count();
		linereader1.close();
		LineNumberReader linereader2 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		count += linereader2.lines().count();
		linereader2.close();
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> data = datastream1.union(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count - 1, sum);
		pipelineconfig.setLocal(local);
		log.info("testUnion After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testUnionFilter() throws Exception {
		log.info("testUnionFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig).map(val -> val);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		List<List<String>> data = datastream1.union(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(504, sum);
		pipelineconfig.setLocal(local);
		log.info("testUnionFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testUnionFilterFilter() throws Exception {
		log.info("testUnionFilterFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		List<List<String>> data = datastream1.union(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(46, sum);
		pipelineconfig.setLocal(local);
		log.info("testUnionFilterFilter After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testIntersection() throws Exception {
		log.info("testIntersection Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader1 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader1.lines().count();
		linereader1.close();
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> data = datastream1.intersection(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testIntersection After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testIntersectionFilter() throws Exception {
		log.info("testIntersectionFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).map(val -> val);
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		List<List<String>> data = datastream1.intersection(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(3, sum);
		pipelineconfig.setLocal(local);
		log.info("testIntersectionFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testIntersectionFilterFilter() throws Exception {
		log.info("testIntersectionFilterFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		StreamPipeline<String> datastream2 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig).filter(val -> "2".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "3".equals(val.split(DataSamudayaConstants.COMMA)[2])
				|| "4".equals(val.split(DataSamudayaConstants.COMMA)[2]));
		List<List<String>> data = datastream1.intersection(datastream2).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(3, sum);
		pipelineconfig.setLocal(local);
		log.info("testIntersectionFilterFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testPeek() throws Exception {
		log.info("testPeek Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader1 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader1.lines().count();
		linereader1.close();
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> data = datastream1.peek(System.out::println).collect(toexecute, null);
		long sum = 0;
		for (int index = 0;index < data.size();index++) {
			sum += data.get(index).size();
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testPeek After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testFilterSorted() throws Exception {
		log.info("testFilterSorted Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader1 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader1.lines().count();
		linereader1.close();
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<String>> datas = datastream1.filter(value -> !"DayofMonth".equals(value.split(DataSamudayaConstants.COMMA)[2])).sorted((val1, val2) -> {
			return Integer.valueOf(val1.split(DataSamudayaConstants.COMMA)[2]).compareTo(Integer.valueOf(val2.split(DataSamudayaConstants.COMMA)[2]));
		}).collect(toexecute, null);
		long sum = 0;
		for (List<String> data :datas) {
			sum += data.size();
			for (int index = 0;index < data.size();index++) {
				if (index > 1) {
					assertTrue(Integer.valueOf(data.get(index - 1).split(DataSamudayaConstants.COMMA)[2]) <= Integer.valueOf(data.get(index).split(DataSamudayaConstants.COMMA)[2]));
				}
			}
		}
		assertEquals(count - 1, sum);
		pipelineconfig.setLocal(local);
		log.info("testFilterSorted After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPair() throws Exception {
		log.info("testMapPair Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		LineNumberReader linereader1 = new LineNumberReader(new InputStreamReader(new FileInputStream("src/test/resources/airlinesamplejoin.csv")));
		long count = linereader1.lines().count();
		linereader1.close();
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
				pipelineconfig);
		List<List<Tuple>> datas = datastream1.mapToPair(value -> Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[8], value.split(DataSamudayaConstants.COMMA)[14]))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple> data :datas) {
			sum += data.size();
			for (Tuple tuple :data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(count, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPair After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFilter() throws Exception {
		log.info("testMapPairFilter Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = datastream1.mapToPair(value -> Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.filter(tup -> tup.v1.equals("1")).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple> data :datas) {
			sum += data.size();
			for (Tuple tuple :data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(3875, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFilter After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFlatMap() throws Exception {
		log.info("testMapPairFlatMap Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMap(tuple -> Arrays.asList(tuple, tuple).stream()).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple> data :datas) {
			sum += data.size();
			for (Tuple tuple :data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(92722, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMap After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFlatMapToLong() throws Exception {
		log.info("testMapPairFlatMapToLong Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMapToLong(tuple -> {
					String val = tuple.v1;
					if (!"Month".equals(val)) {
						return Arrays.asList(Tuple.tuple(Long.parseLong(val)
						, Long.parseLong(val)));
					}
					return Arrays.asList(Tuple.tuple(0l, 0l));
				})
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple> data :datas) {
			sum += data.size();
			for (Tuple tuple :data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToLong After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFlatMapToDouble() throws Exception {
		log.info("testMapPairFlatMapToDouble Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple>> datas = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], value.split(DataSamudayaConstants.COMMA)[14]))
				.flatMapToDouble(tuple -> {
					String val = tuple.v1;
					if (!"Month".equals(val)) {
						return Arrays.asList(Tuple.tuple(Double.parseDouble(val)
						, Double.parseDouble(val))).stream();
					}
					return Arrays.asList(Tuple.tuple(0d, 0d)).stream();
				})
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple> data :datas) {
			sum += data.size();
			for (Tuple tuple :data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToDouble After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairCoalesce() throws Exception {
		log.info("testMapPairCoalesce Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).coalesce(1, (a, b) -> a + b).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {
			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				sum += tuple.v2;
			}
		}
		assertEquals(-63278l, sum);
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairCoalesce After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairReduceByKey() throws Exception {
		log.info("testMapPairReduceByKey Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).reduceByKey((a, b) -> a + b).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {
			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				sum += tuple.v2;
			}
		}
		assertEquals(-63278l, sum);
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairReduceByKey After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairSample() throws Exception {
		log.info("testMapPairSample Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).sample(100).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {
			sum += data.size();
			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(100, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairSample After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairPeek() throws Exception {
		log.info("testMapPairPeek Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).peek(System.out::println).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {
			sum += data.size();
			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(46361l, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairPeek After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFoldLeft() throws Exception {
		log.info("testMapPairFoldLeft Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).foldLeft(0l, (a, b) -> a + b, 1, (a, b) -> a + b).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {

			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				sum += tuple.v2;
			}
		}
		assertEquals(-63278l, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldLeft After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairFoldRight() throws Exception {
		log.info("testMapPairFoldRight Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, Long>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).foldRight(0l, (a, b) -> a + b, 1, (a, b) -> a + b).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Long>> data : datas) {

			for (Tuple2<String, Long> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				sum += tuple.v2;
			}
		}
		assertEquals(-63278l, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldRight After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairGroupByKey() throws Exception {
		log.info("testMapPairGroupByKey Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Tuple2<String, List<Long>>>> datas = datastream1.mapToPair(value -> {
			String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
			if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], Long.parseLong(arrdelay));
			} else {
				return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
			}
		}).groupByKey().collect(toexecute, null);
		long sum = 0;
		long totalrec = 0;
		for (List<Tuple2<String, List<Long>>> data : datas) {

			for (Tuple2<String, List<Long>> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				totalrec += tuple.v2.size();
				for (long val :tuple.v2) {
					sum += val;
				}
			}
		}
		assertEquals(46361l, totalrec);
		assertEquals(-63278l, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairGroupByKey After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>> datas = mtsl1.join(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.collect(toexecute, null);
		long totalrec = 0;
		for (List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> data : datas) {
			totalrec += data.size();
			for (Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
			}
		}
		assertEquals(1492l, totalrec);
		pipelineconfig.setLocal(local);
		log.info("testMapPairJoin After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>> datas = mtsl1.leftOuterjoin(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.collect(toexecute, null);
		long totalrec = 0;
		for (List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> data : datas) {
			totalrec += data.size();
			for (Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
			}
		}
		assertEquals(1492l, totalrec);
		pipelineconfig.setLocal(local);
		log.info("testMapPairLeftOuterJoin After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>>> datas = mtsl1.rightOuterjoin(mtsl2, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1))
				.collect(toexecute, null);
		long totalrec = 0;
		for (List<Tuple2<Tuple2<String, String>, Tuple2<String, String>>> data : datas) {
			totalrec += data.size();
			for (Tuple2<Tuple2<String, String>, Tuple2<String, String>> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				assertTrue(tuple.v1.v1.equals(tuple.v2.v1));
			}
		}
		assertEquals(1492l, totalrec);
		pipelineconfig.setLocal(local);
		log.info("testMapPairRightOuterJoin After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<String, String>>> datas = mtsl1.union(mtsl2)
				.collect(toexecute, null);
		long totalrec = 0;
		for (List<Tuple2<String, String>> data : datas) {
			totalrec += data.size();
			for (Tuple2<String, String> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(1492l, totalrec);
		pipelineconfig.setLocal(local);
		log.info("testMapPairUnion After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<String, String>>> datas = mtsl1.intersection(mtsl2)
				.collect(toexecute, null);
		long totalrec = 0;
		for (List<Tuple2<String, String>> data : datas) {
			totalrec += data.size();
			for (Tuple2<String, String> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
			}
		}
		assertEquals(1492l, totalrec);
		pipelineconfig.setLocal(local);
		log.info("testMapPairIntersection After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
	public void testMapPairSorted() throws Exception {
		log.info("testMapPairSorted Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
				pipelineconfig);
		List<List<Tuple2<String, String>>> datas = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1])).sorted((tuple1, tuple2) -> ((String) tuple1.v1).compareTo((String) tuple2.v1)).collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, String>> data : datas) {
			sum += data.size();
			for (Tuple2<String, String> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				log.info(tuple);
			}
		}
		assertEquals(1492l, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapPairSorted After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked"})
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
		List<List<Tuple2<Tuple2<String, List<Long>>, Tuple2<String, List<Long>>>>> datas = data1.cogroup(data2).collect(toexecute,
				null);
		int sum = 0;
		for (List<Tuple2<Tuple2<String, List<Long>>, Tuple2<String, List<Long>>>> data : datas) {
			for (Tuple2<Tuple2<String, List<Long>>, Tuple2<String, List<Long>>> tuple : data) {
				assertTrue(tuple instanceof Tuple2);
				sum += tuple.v1.v2.size();
				log.info(tuple);
			}
		}
		assertEquals(45957l, sum);
		pipelineconfig.setLocal(local);
		log.info("testFilterMapPairCogroup After---------------------------------------");
	}

}
