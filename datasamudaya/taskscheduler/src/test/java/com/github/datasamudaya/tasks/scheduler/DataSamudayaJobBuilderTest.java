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
package com.github.datasamudaya.tasks.scheduler;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import java.util.List;
import org.junit.Test;

import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplication;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationBuilder;

public class DataSamudayaJobBuilderTest extends MassiveDataMRJobBase {

	@Test
	public void testDataSamudayaJobBuilder() {
		MapReduceApplication datasamudayajob = (MapReduceApplication) MapReduceApplicationBuilder.newBuilder()
				.addMapper(AirlineDataMapper.class, "/carriers")
				.addMapper(AirlineDataMapper.class, "/airlines")
				.addCombiner(AirlineDataMapper.class)
				.addReducer(AirlineDataMapper.class)
				.setOutputfolder("/aircararrivaldelay")
				.build();
		assertTrue(datasamudayajob.mappers.get(0).crunchmapper == AirlineDataMapper.class);
		assertEquals("/carriers", datasamudayajob.mappers.get(0).inputfolderpath);
		assertTrue(datasamudayajob.mappers.get(1).crunchmapper == AirlineDataMapper.class);
		assertEquals("/airlines", datasamudayajob.mappers.get(1).inputfolderpath);
		assertTrue(datasamudayajob.combiners.get(0) == AirlineDataMapper.class);
		assertTrue(datasamudayajob.reducers.get(0) == AirlineDataMapper.class);
		assertEquals("/aircararrivaldelay", datasamudayajob.outputfolder);
	}

	@SuppressWarnings({"rawtypes"})
	@Test
	public void testDataSamudayaJobCall() {
		MapReduceApplication datasamudayajob = (MapReduceApplication) MapReduceApplicationBuilder.newBuilder()
				.addMapper(new AirlineDataMapper(), "/airlinesample")
				.addCombiner(new AirlineDataMapper())
				.addReducer(new AirlineDataMapper())
				.setOutputfolder("/aircararrivaldelay")
				.build();
		assertTrue(datasamudayajob.mappers.get(0).crunchmapper instanceof AirlineDataMapper);
		assertEquals("/airlinesample", datasamudayajob.mappers.get(0).inputfolderpath);
		assertTrue(datasamudayajob.combiners.get(0) instanceof AirlineDataMapper);
		assertTrue(datasamudayajob.reducers.get(0) instanceof AirlineDataMapper);
		assertEquals("/aircararrivaldelay", datasamudayajob.outputfolder);
		List<DataCruncherContext> dccl = datasamudayajob.call();
		log.info(dccl);
		assertEquals(1, dccl.size());
	}
}
