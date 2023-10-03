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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.github.datasamudaya.stream.StreamPipeline;

public class StreamPipelineUserDefinedObjectsTest extends StreamPipelineBaseTestCommon {
	boolean toexecute = true;
	int sum;
	static Logger log = Logger.getLogger(StreamPipelineUserDefinedObjectsTest.class);

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapCollect() throws Throwable {
		log.info("testMapCollect Before---------------------------------------");
		String local = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Map>> data = (List<List<Map>>) datapipeline.map(value -> value.split(","))
				.map(val -> {
					Map<String, String> map = new HashMap<>();
					int index = 0;
					for (String header :airlineheader) {
						map.put(header, val[index]);
						index++;
					}
					return map;

				}).collect(toexecute, null);
		int sum = 0;
		for (List<Map> partitioneddata : data) {
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(local);
		log.info("testMapCollect After---------------------------------------");
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testMapContainerExecuteCollect() throws Throwable {
		log.info("testMapContainerExecuteCollect Before---------------------------------------");
		String localmode = pipelineconfig.getLocal();
		pipelineconfig.setLocal("true");
		StreamPipeline<String> datapipeline = StreamPipeline.newStreamHDFS(hdfsfilepath,
				airlinesample, pipelineconfig);
		List<List<Map>> data = (List<List<Map>>) datapipeline.map(value -> value.split(","))
				.map(val -> {
					Map<String, String> map = new HashMap<>();
					int index = 0;
					for (String header :airlineheader) {
						map.put(header, val[index]);
						index++;
					}
					return map;

				}).collect(toexecute, null);
		int sum = 0;
		for (List<Map> partitioneddata : data) {
			sum += partitioneddata.size();
		}
		assertEquals(46361, sum);
		pipelineconfig.setLocal(localmode);
		log.info("testMapContainerExecuteCollect After---------------------------------------");
	}
}
