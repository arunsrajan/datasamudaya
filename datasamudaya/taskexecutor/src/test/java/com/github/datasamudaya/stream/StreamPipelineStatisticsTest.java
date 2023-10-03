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

import static org.junit.Assert.assertTrue;
import java.util.IntSummaryStatistics;
import java.util.List;
import org.apache.log4j.Logger;
import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.stream.StreamPipeline;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineStatisticsTest extends StreamPipelineBaseTestCommon {

	Logger log = Logger.getLogger(StreamPipelineStatisticsTest.class);
	boolean toexecute = true;

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testSummaryStatistics() throws Throwable {
		log.info("testSummaryStatistics Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<IntSummaryStatistics>> summarylist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).summaryStatistics();
		for (List<IntSummaryStatistics> sumstats : summarylist) {
			for (IntSummaryStatistics summary : sumstats) {
				log.info(summary);
			}
			log.info("");
		}
		assertTrue(summarylist.size() > 0);
		log.info("testSummaryStatistics After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testSummaryStatisticsPartitioned() throws Throwable {
		log.info("testSummaryStatisticsPartitioned Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<IntSummaryStatistics>> summarylist = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).summaryStatistics();
		for (List<IntSummaryStatistics> sumstats : summarylist) {
			for (IntSummaryStatistics summary : sumstats) {
				log.info(summary);
			}
			log.info("");
		}
		assertTrue(summarylist.size() > 0);
		pipelineconfig.setBlocksize("20");
		log.info("testSummaryStatisticsPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testSum() throws Throwable {
		log.info("testSum Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> suml = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).sum();
		for (List<Integer> sums : suml) {
			for (Integer sum : sums) {
				log.info(sum);
			}
			log.info("");
		}
		assertTrue(suml.size() > 0);
		log.info("testSum After---------------------------------------");
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testSumPartitioned() throws Throwable {
		log.info("testSumPartitioned Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> suml = (List<List<Integer>>) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).sum();
		for (List<Integer> sums : suml) {
			for (Integer sum : sums) {
				log.info(sum);
			}
			log.info("");
		}
		assertTrue(suml.size() > 0);
		pipelineconfig.setBlocksize("20");
		log.info("testSumPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMax() throws Throwable {
		log.info("testMax Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> maxl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).max();
		for (List<Integer> maxs : maxl) {
			for (Integer max : maxs) {
				log.info(max);
			}
			log.info("");
		}
		assertTrue(maxl.size() > 0);
		log.info("testMax After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMaxPartitioned() throws Throwable {
		log.info("testMaxPartitioned Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> maxl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).max();
		for (List<Integer> maxs : maxl) {
			for (Integer max : maxs) {
				log.info(max);
			}
			log.info("");
		}
		assertTrue(maxl.size() > 0);
		pipelineconfig.setBlocksize("20");
		log.info("testMaxPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMin() throws Throwable {
		log.info("testMin Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> minl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).min();
		for (List<Integer> mins : minl) {
			for (Integer min : mins) {
				log.info(min);
			}
			log.info("");
		}
		assertTrue(minl.size() > 0);
		log.info("testMin After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMinPartitioned() throws Throwable {
		log.info("testMinPartitioned Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Integer>> minl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).min();
		for (List<Integer> mins : minl) {
			for (Integer min : mins) {
				log.info(min);
			}
			log.info("");
		}
		assertTrue(minl.size() > 0);
		pipelineconfig.setBlocksize("20");
		log.info("testMinPartitioned After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testStandardDeviation() throws Throwable {
		log.info("testStandardDeviation Before---------------------------------------");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> stdl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).standardDeviation();
		for (List<Double> stds : stdl) {
			for (Double std : stds) {
				log.info(std);
			}
			log.info("");
		}
		assertTrue(stdl.size() > 0);
		log.info("testStandardDeviation After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testStandardDeviationPartitioned() throws Throwable {
		log.info("testStandardDeviationPartitioned Before---------------------------------------");
		pipelineconfig.setBlocksize("1");
		StreamPipeline<String> datastream = StreamPipeline.newStreamHDFS(hdfsfilepath, airlinesample,
				pipelineconfig);
		List<List<Double>> stdl = (List) datastream.map(str -> str.split(","))
				.filter(str -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14])).mapToInt(str -> Integer.parseInt(str[14])).standardDeviation();
		for (List<Double> stds : stdl) {
			for (Double std : stds) {
				log.info(std);
			}
			log.info("");
		}
		assertTrue(stdl.size() > 0);
		pipelineconfig.setBlocksize("20");
		log.info("testStandardDeviationPartitioned After---------------------------------------");
	}
}
