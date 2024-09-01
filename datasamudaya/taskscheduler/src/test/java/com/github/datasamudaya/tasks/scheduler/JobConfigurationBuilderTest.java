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
import org.junit.Test;

import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobConfigurationBuilder;
import com.github.datasamudaya.common.DataSamudayaConstants;

public class JobConfigurationBuilderTest extends TaskSchedulerCommon {

	@Test
	public void testJobConfigurationBuilder() {
		JobConfiguration jc = JobConfigurationBuilder.newBuilder().build();
		assertEquals("2", jc.getBatchsize());
		assertEquals(DataSamudayaConstants.GCCONFIG_DEFAULT, jc.getGctype());
		assertEquals("hdfs://127.0.0.1:9000", jc.getHdfsurl());
		assertEquals("1024", jc.getMaxmem());
		assertEquals("1024", jc.getMinmem());
		assertEquals(null, jc.getMrjar());
		assertEquals(DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT, jc.getNumberofcontainers());
		assertEquals("1", jc.getNumofreducers());
		assertEquals(null, jc.getOutput());
		assertEquals("127.0.0.1", jc.getTshost());
		assertEquals("11111", jc.getTsport());
		assertEquals(null, jc.getTstempdir());
		assertEquals("127.0.0.1:2181", jc.getZkport());
		assertEquals("2000", jc.getZkretrydelay());
	}

}
