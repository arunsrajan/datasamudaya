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
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;

public class StreamPipelineTransformationsNullTest extends StreamPipelineBaseTestCommon {

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
	public void testMapNull() throws Exception {
		log.info("testMapNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.map(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.MAPFUNCTIONNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapNull After---------------------------------------");
	}

	@Test

	public void testFilterNull() throws Exception {
		log.info("testFilterNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.filter(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.PREDICATENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testFilterNull After---------------------------------------");
	}

	@Test

	public void testflatMapNull() throws Exception {
		log.info("testflatMapNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.flatMap(null).collect(toexecute, null);

		} catch (Exception ex) {
			assertEquals(PipelineConstants.FLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testflatMapNull After---------------------------------------");
	}

	@Test

	public void testflatMapToDoubleNull() throws Exception {
		log.info("testflatMapToDoubleNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.flatMapToDouble(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.DOUBLEFLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testflatMapToDoubleNull After---------------------------------------");
	}

	@Test

	public void testflatMapToLongNull() throws Exception {
		log.info("testflatMapToLongNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.flatMapToLong(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LONGFLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testflatMapToLongNull After---------------------------------------");
	}

	@Test

	public void testflatMapToTuple2Null() throws Exception {
		log.info("testflatMapToTuple2Null Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.flatMapToTuple2(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FLATMAPPAIRNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testflatMapToTuple2Null After---------------------------------------");
	}

	@Test

	public void testflatMapToTupleNull() throws Exception {
		log.info("testflatMapToTupleNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.flatMapToTuple(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FLATMAPPAIRNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testflatMapToTupleNull After---------------------------------------");
	}

	@Test

	public void testKeyByFunctionNull() throws Exception {
		log.info("testKeyByFunctionNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream.keyBy(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.KEYBYNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testKeyByFunctionNull After---------------------------------------");
	}

	@Test

	public void testLeftOuterJoinNull() throws Exception {
		log.info("testLeftOuterJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.leftOuterjoin(null, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LEFTOUTERJOIN, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoinNull After---------------------------------------");
	}

	@Test

	public void testLeftOuterJoinNullCondition() throws Exception {
		log.info("testLeftOuterJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.leftOuterjoin(datastream1, (LeftOuterJoinPredicate)null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LEFTOUTERJOINCONDITION, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testLeftOuterJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testJoinNull() throws Exception {
		log.info("testJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.join(null, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INNERJOIN, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testJoinNull After---------------------------------------");
	}

	@Test

	public void testJoinNullCondition() throws Exception {
		log.info("testJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.join(datastream1, (JoinPredicate)null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INNERJOINCONDITION, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testRightOuterJoinNull() throws Exception {
		log.info("testRightOuterJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		try {
			pipelineconfig.setLocal("true");
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.rightOuterjoin(null, (val1, val2) -> val1.equals(val2)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.RIGHTOUTERJOIN, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinNull After---------------------------------------");
	}

	@Test

	public void testRightOuterJoinNullCondition() throws Exception {
		log.info("testRightOuterJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.rightOuterjoin(datastream1, (RightOuterJoinPredicate)null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.RIGHTOUTERJOINCONDITION, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testRightOuterJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testUnionNull() throws Exception {
		log.info("testUnionNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1987,
					pipelineconfig);
			datastream1.union(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.UNIONNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testUnionNull After---------------------------------------");
	}

	@Test

	public void testIntersectionNull() throws Exception {
		log.info("testIntersectionNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.intersection(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INTERSECTIONNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testIntersectionNull After---------------------------------------");
	}

	@Test

	public void testPeekNull() throws Exception {
		log.info("testPeekNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.peek(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.PEEKNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testPeekNull After---------------------------------------");
	}

	@Test

	public void testFilterSortedNull() throws Exception {
		log.info("testFilterSortedNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.filter(value -> !"DayofMonth".equals(value.split(DataSamudayaConstants.COMMA)[2])).sorted(null)
					.collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.SORTEDNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testFilterSortedNull After---------------------------------------");
	}

	@Test

	public void testMapPairNull() throws Exception {
		log.info("testMapPairNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesamplejoin,
					pipelineconfig);
			datastream1.mapToPair(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.MAPPAIRNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairNull After---------------------------------------");
	}

	@Test

	public void testMapPairFlatMapNull() throws Exception {
		log.info("testMapPairFlatMapNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
					value.split(DataSamudayaConstants.COMMA)[14])).flatMap(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapNull After---------------------------------------");
	}

	@Test

	public void testMapPairFlatMapToLongNull() throws Exception {
		log.info("testMapPairFlatMapToLongNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
					value.split(DataSamudayaConstants.COMMA)[14])).flatMapToLong(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LONGFLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToLongNull After---------------------------------------");
	}

	@Test

	public void testMapPairFlatMapToDoubleNull() throws Exception {
		log.info("testMapPairFlatMapToDoubleNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
					value.split(DataSamudayaConstants.COMMA)[14])).flatMapToDouble(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.DOUBLEFLATMAPNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFlatMapToDoubleNull After---------------------------------------");
	}

	@Test

	public void testMapPairCoalesceNull() throws Exception {
		log.info("testMapPairCoalesceNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).coalesce(1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.COALESCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairCoalesceNull After---------------------------------------");
	}

	@Test

	public void testMapPairReduceByKeyNull() throws Exception {
		log.info("testMapPairReduceByKeyNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		String blocksize = pipelineconfig.getBlocksize();
		pipelineconfig.setBlocksize("1");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).reduceByKey(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.REDUCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		pipelineconfig.setBlocksize(blocksize);
		log.info("testMapPairReduceByKeyNull After---------------------------------------");
	}

	@Test

	public void testMapPairSampleNull() throws Exception {
		log.info("testMapPairSampleNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).sample(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.SAMPLENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairSampleNull After---------------------------------------");
	}

	@Test

	public void testMapPairPeekNull() throws Exception {
		log.info("testMapPairPeekNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).peek(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.PEEKNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairPeekNull After---------------------------------------");
	}

	@Test

	public void testMapPairFoldLeftReduceNull() throws Exception {
		log.info("testMapPairFoldLeftReduceNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).foldLeft(0l, null, 1, (a, b) -> a + b).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FOLDLEFTREDUCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldLeftReduceNull After---------------------------------------");
	}

	@Test

	public void testMapPairFoldLeftCoalesceNull() throws Exception {
		log.info("testMapPairFoldLeftCoalesceNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).foldLeft(0l, (a, b) -> a + b, 1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FOLDLEFTCOALESCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldLeftCoalesceNull After---------------------------------------");
	}

	@Test

	public void testMapPairFoldRightReduceNull() throws Exception {
		log.info("testMapPairFoldRightReduceNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).foldRight(0l, null, 1, (a, b) -> a + b).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FOLDRIGHTREDUCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldRightReduceNull After---------------------------------------");
	}

	@Test

	public void testMapPairFoldRightCoalesceNull() throws Exception {
		log.info("testMapPairFoldRightCoalesceNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
					pipelineconfig);
			datastream1.mapToPair(value -> {
				String arrdelay = value.split(DataSamudayaConstants.COMMA)[14];
				if (!"ArrDelay".equals(arrdelay) && !"NA".equals(arrdelay)) {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1],
							Long.parseLong(arrdelay));
				} else {
					return (Tuple2<String, Long>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[1], 0l);
				}
			}).foldRight(0l, (a, b) -> a + b, 1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.FOLDRIGHTCOALESCENULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairFoldRightCoalesceNull After---------------------------------------");
	}

	@Test

	public void testMapPairJoinNull() throws Exception {
		log.info("testMapPairJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.join(null, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INNERJOIN, ex.getMessage());
		}

		pipelineconfig.setLocal(local);
		log.info("testMapPairJoinNull After---------------------------------------");
	}

	@Test

	public void testMapPairJoinNullCondition() throws Exception {
		log.info("testMapPairJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.join(datastream1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INNERJOINCONDITION, ex.getMessage());
		}

		pipelineconfig.setLocal(local);
		log.info("testMapPairJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testMapPairLeftOuterJoinNull() throws Exception {
		log.info("testMapPairLeftOuterJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.leftOuterjoin(null, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LEFTOUTERJOIN, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairLeftOuterJoinNull After---------------------------------------");
	}

	@Test

	public void testMapPairLeftOuterJoinNullCondition() throws Exception {
		log.info("testMapPairLeftOuterJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.leftOuterjoin(mtsl1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.LEFTOUTERJOINCONDITION, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairLeftOuterJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testMapPairRightOuterJoinNull() throws Exception {
		log.info("testMapPairRightOuterJoinNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.rightOuterjoin(null, (tuple1, tuple2) -> tuple1.v1.equals(tuple2.v1)).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.RIGHTOUTERJOIN, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairRightOuterJoinNull After---------------------------------------");
	}

	@Test

	public void testMapPairRightOuterJoinNullCondition() throws Exception {
		log.info("testMapPairRightOuterJoinNullCondition Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.rightOuterjoin(mtsl1, null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.RIGHTOUTERJOINCONDITION, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairRightOuterJoinNullCondition After---------------------------------------");
	}

	@Test

	public void testMapPairUnionNull() throws Exception {
		log.info("testMapPairUnionNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.union(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.UNIONNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairUnionNull After---------------------------------------");
	}

	@Test

	public void testMapPairIntersectionNull() throws Exception {
		log.info("testMapPairIntersectionNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			MapPair<String, String> mtsl1 = datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple
					.tuple(value.split(DataSamudayaConstants.COMMA)[0], value.split(DataSamudayaConstants.COMMA)[1]));
			mtsl1.intersection(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.INTERSECTIONNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairIntersectionNull After---------------------------------------");
	}

	@Test

	public void testMapPairSortedNull() throws Exception {
		log.info("testMapPairSortedNull Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		try {
			StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers,
					pipelineconfig);
			datastream1.mapToPair(value -> (Tuple2<String, String>) Tuple.tuple(value.split(DataSamudayaConstants.COMMA)[0],
					value.split(DataSamudayaConstants.COMMA)[1])).sorted(null).collect(toexecute, null);
		} catch (Exception ex) {
			assertEquals(PipelineConstants.SORTEDNULL, ex.getMessage());
		}
		pipelineconfig.setLocal(local);
		log.info("testMapPairSortedNull After---------------------------------------");
	}

}
