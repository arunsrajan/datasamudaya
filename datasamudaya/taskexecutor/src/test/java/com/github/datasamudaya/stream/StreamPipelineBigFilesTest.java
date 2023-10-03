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
import java.net.URI;
import java.util.List;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.junit.Test;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.stream.MapPair;
import com.github.datasamudaya.stream.StreamPipeline;

public class StreamPipelineBigFilesTest extends StreamPipelineBaseTestCommon {


	boolean toexecute = true;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValues() throws Throwable {
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("5");
		log.info("testMapValuesReduceByValues Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(41630119l, sum);
		log.info("testMapValuesReduceByValues After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValuesBigger() throws Throwable {
		pipelineconfig.setLocal("true");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("3");
		log.info("testMapValuesReduceByValuesBigger Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l)).reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(41630119l, sum);
		log.info("testMapValuesReduceByValuesBigger After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValuesCoalesce() throws Throwable {
		log.info("testMapValuesReduceByValuesCoalesce Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("4");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(41630119l, sum);
		pipelineconfig.setBlocksize("20");
		log.info("testMapValuesReduceByValuesCoalesce After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValuesJGroups() throws Throwable {
		log.info("testMapValuesReduceByValuesJGroups Before---------------------------------------");
		pipelineconfig.setJgroups("true");
		pipelineconfig.setLocal("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("4");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(852674931, sum);
		log.info("testMapValuesReduceByValuesJGroups After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValuesLocal() throws Throwable {
		log.info("testMapValuesReduceByValuesJGroups Before---------------------------------------");
		pipelineconfig.setLocal("true");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("4");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(852674931, sum);
		log.info("testMapValuesReduceByValuesJGroups After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapValuesReduceByValuesCoalesceJGroups() throws Throwable {
		log.info("testMapValuesReduceByValuesCoalesce Before---------------------------------------");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setLocal("false");
		pipelineconfig.setJgroups("true");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		assertEquals(313881010, sum);
		pipelineconfig.setBlocksize("20");
		log.info("testMapValuesReduceByValuesCoalesce After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoinUserDefinedBlockSize() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setStorage(STORAGE.INMEMORY_DISK);
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).saveAsTextFile(new URI(hdfsfilepath), "/coalesce/Coalesce-" + System.currentTimeMillis());
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSize After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoinUserDefinedBlockSizeCollect() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSizeCollect Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setStorage(STORAGE.INMEMORY_DISK);
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));
		List<List<Tuple2>> tuples2 = carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples :tuples2) {
			for (Tuple2 tuple :tuples) {
				log.info(tuple);
			}
		}
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSizeCollect After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoinUserDefinedBlockSizeJGroups() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSizeJGroups Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setJgroups("true");
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setStorage(STORAGE.DISK);
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).saveAsTextFile(new URI(hdfsfilepath), "/coalesce/Coalesce-" + System.currentTimeMillis());
		log.info("testReduceByKeyCoalesceJoinUserDefinedBlockSizeJGroups After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoin() throws Throwable {
		log.info("testReduceByKeyCoalesceJoin Before---------------------------------------");
		pipelineconfig.setLocal("true");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setBlocksize("64");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, "/1987",
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).saveAsTextFile(new URI(hdfsfilepath), "/coalesce/Coalesce-" + System.currentTimeMillis());
		log.info("testReduceByKeyCoalesceJoin After---------------------------------------");
	}


	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoinJGroups() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinJGroups Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setJgroups("true");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setBlocksize("128");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1)).saveAsTextFile(new URI(hdfsfilepath), "/coalesce/Coalesce-" + System.currentTimeMillis());
		log.info("testReduceByKeyCoalesceJoinJGroups After---------------------------------------");
	}

	@Test
	@SuppressWarnings({"unchecked", "rawtypes"})
	public void testReduceByKeyCoalesceJoinJGroupsCollect() throws Throwable {
		log.info("testReduceByKeyCoalesceJoinJGroupsCollect Before---------------------------------------");
		pipelineconfig.setLocal("false");
		pipelineconfig.setJgroups("true");
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setBlocksize("128");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airline1989,
				pipelineconfig);
		MapPair<String, Long> mappair1 = (MapPair) datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		MapPair<String, Long> airlinesamples = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);

		StreamPipeline<String> datastream1 = StreamPipeline.newStreamHDFS(hdfsfilepath, carriers, pipelineconfig);

		MapPair<Tuple, Object> carriers = datastream1.map(linetosplit -> linetosplit.split(","))
				.mapToPair(line -> new Tuple2(line[0].substring(1, line[0].length() - 1),
						line[1].substring(1, line[1].length() - 1)));

		List<List<Tuple2>> tuples2 = carriers
				.join(airlinesamples, (tuple1, tuple2) -> ((Tuple2) tuple1).v1.equals(((Tuple2) tuple2).v1))
				.collect(toexecute, null);
		for (List<Tuple2> tuples :tuples2) {
			for (Tuple2 tuple :tuples) {
				log.info(tuple);
			}
		}
		log.info("testReduceByKeyCoalesceJoinJGroupsCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testFilterCollect() throws Throwable {
		log.info("testFilterCollect Before---------------------------------------");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlines, pipelineconfig);
		List<List> data = (List) datapipeline
				.filter(val -> "1987".equals(val.split(DataSamudayaConstants.COMMA)[0])).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		log.info(sum);
		assertEquals(1311826, sum);
		log.info("testFilterCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testResourcesAllocationBeforeAndAfterExecCombined() throws Throwable {
		log.info("testResourcesAllocationBeforeAndAfterExecCombined Before---------------------------------------");
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("true");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("2");
		log.info(DataSamudayaNodesResources.get());
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		log.info(DataSamudayaNodesResources.get());
		log.info("testResourcesAllocationBeforeAndAfterExecCombined After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testResourcesAllocationBeforeAndAfterExecDivided() throws Throwable {
		log.info("testResourcesAllocationBeforeAndAfterExecDivided Before---------------------------------------");
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("true");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY_DISK);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("64");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("1");
		pipelineconfig.setContaineralloc("DIVIDED");
		log.info(DataSamudayaNodesResources.get());
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, "/test3gb",
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		log.info(DataSamudayaNodesResources.get());
		log.info("testResourcesAllocationBeforeAndAfterExecDivided After---------------------------------------");
	}


	@Test
	public void testResourcesAllocationBeforeAndAfterExecCombinedCombinedCombined() throws Throwable {
		Thread thr1 = new Thread(() -> {
			try {
				testResourcesAllocationBeforeAndAfterExecCombined();
			} catch (Throwable e) {
			}
		});
		Thread thr2 = new Thread(() -> {
			try {
				testResourcesAllocationBeforeAndAfterExecCombined();
			} catch (Throwable e) {
			}
		});
		Thread thr3 = new Thread(() -> {
			try {
				testResourcesAllocationBeforeAndAfterExecCombined();
			} catch (Throwable e) {
			}
		});
		thr1.start();
		thr2.start();
		thr3.start();
		thr1.join();
		thr2.join();
		thr3.join();
	}

	@Test
	public void testFilterFilterSaveAsTextFile() throws Exception {
		log.info("testFilterFilterSaveAsTextFile Before---------------------------------------");
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("true");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setMaxmem("3072");
		pipelineconfig.setMinmem("512");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setNumberofcontainers("1");
		pipelineconfig.setBatchsize("4");
		pipelineconfig.setContaineralloc("COMBINE");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		datastream
				.filter(value -> !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]))
				.filter(value -> !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]))
				.saveAsTextFile(new URI(hdfsfilepath), "/filtertest/FilterFilter-" + System.currentTimeMillis());
		log.info("testFilterFilterSaveAsTextFile After---------------------------------------");
	}


	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testResourcesAllocationCoalesceExecDivided() throws Throwable {
		log.info("testResourcesAllocationCoalesceExecDivided Before---------------------------------------");
		PipelineConfig pipelineconfig = new PipelineConfig();
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setStorage(DataSamudayaConstants.STORAGE.INMEMORY);
		pipelineconfig.setIsblocksuserdefined("true");
		pipelineconfig.setBlocksize("128");
		pipelineconfig.setGctype(DataSamudayaConstants.ZGC);
		pipelineconfig.setMode(DataSamudayaConstants.NORMAL);
		pipelineconfig.setNumberofcontainers("5");
		pipelineconfig.setBatchsize("2");
		pipelineconfig.setContaineralloc("DIVIDED");
		log.info(DataSamudayaNodesResources.get());
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlines,
				pipelineconfig);
		List<List<Tuple2<String, Tuple2<Long, Long>>>> redByKeyList = (List) datastream.map(dat -> dat.split(","))
				.filter(dat -> dat != null && !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> (Tuple2<String, Long>) Tuple.tuple(dat[8], Long.parseLong(dat[14])))
				.mapValues(mv -> new Tuple2<Long, Long>(mv, 1l))
				.reduceByValues((tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.coalesce(1, (tuple1, tuple2) -> new Tuple2<Long, Long>(tuple1.v1 + tuple2.v1, tuple1.v2 + tuple2.v2))
				.collect(toexecute, null);
		long sum = 0;
		for (List<Tuple2<String, Tuple2<Long, Long>>> tuples : redByKeyList) {
			for (Tuple2<String, Tuple2<Long, Long>> pair : tuples) {
				log.info(pair);
				sum += (Long) pair.v2.v1;
			}
		}
		log.info(sum);
		log.info(DataSamudayaNodesResources.get());
		log.info("testResourcesAllocationCoalesceExecDivided After---------------------------------------");
	}
}
