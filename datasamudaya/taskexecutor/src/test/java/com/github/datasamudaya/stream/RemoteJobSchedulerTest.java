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

import java.io.Serializable;
import java.util.List;

import org.junit.FixMethodOrder;
import org.junit.Test;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;

@SuppressWarnings({"unchecked", "serial", "rawtypes"})
@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class RemoteJobSchedulerTest extends StreamPipelineBaseTestCommon implements Serializable{
	boolean toexecute = true;
	int sum;

	@Test
	public void testFilterFilterCollect() throws Throwable {		
		log.info("testFilterFilter Before---------------------------------------");
		PipelineConfig pc = new PipelineConfig();
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		pc.setLocal("false");
		pc.setIsremotescheduler(true);
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pc);
		List<List> data = (List) datapipeline
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
			}
		}).collect(toexecute, null);
		int sum = 0;
		for (List partitioneddata : data) {
			log.info(partitioneddata.size());
			sum += partitioneddata.size();
		}
		assertEquals(45957, sum);

		log.info("testFilterFilter After---------------------------------------");
	}

	@Test
	public void testFilterFilterCount() throws Throwable {
		log.info("testFilterFilterCount Before---------------------------------------");
		PipelineConfig pc = new PipelineConfig();
		pc.setMode(DataSamudayaConstants.MODE_NORMAL);
		pc.setLocal("false");
		pc.setIsremotescheduler(true);
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pc);
		List<List<Long>> data = (List) datapipeline
				.filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String>() {
					public boolean test(java.lang.String value) {
						return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
					}
				}).filter(new com.github.datasamudaya.common.functions.PredicateSerializable<java.lang.String>() {
			public boolean test(java.lang.String value) {
				return !"NA".equals(value.split(",")[14]) && !"ArrDelay".equals(value.split(",")[14]);
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

		log.info("testFilterFilterCount After---------------------------------------");
	}

}
