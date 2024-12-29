/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.stream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.FixMethodOrder;
import org.junit.runners.MethodSorters;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;

@FixMethodOrder(MethodSorters.NAME_ASCENDING)
public class StreamPipelineSqlBuilderTest extends StreamPipelineSqlBuilderLocalModeTest {	
	Logger log = LogManager.getLogger(StreamPipelineSqlBuilderTest.class);
	@AfterClass
	public static void pipelineConfigReset() throws Exception {
		pipelineconfig.setStorage(STORAGE.INMEMORY);
		pipelineconfig.setLocal("true");
		pipelineconfig.setBlocksize("20");
	}
	@BeforeClass
	public static void pipelineSetup() throws Exception, Throwable {
		DataSamudayaProperties.get().setProperty(DataSamudayaConstants.CLASSPATHFOLDER, DataSamudayaConstants.CLASSPATHFOLDER_DEFAULT);
		pipelineconfig.setLocal("false");
		pipelineconfig.setBlocksize("1");
		pipelineconfig.setBatchsize(DataSamudayaConstants.EMPTY + Runtime.getRuntime().availableProcessors());
		if ("false".equals(pipelineconfig.getLocal())) {
			pipelineconfig.setUseglobaltaskexecutors(true);
			pipelineconfig.setIsremotescheduler(true);
			pipelineconfig.setTejobid(tejobid);
			Utils.launchContainersExecutorSpecWithDriverSpec("arun", tejobid, 1, 3000, 2, 1, 3000, true);
			pipelineconfig.setUser("arun");
		}
	}
	
}
