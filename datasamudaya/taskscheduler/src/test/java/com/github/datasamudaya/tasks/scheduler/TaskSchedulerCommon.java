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

import org.burningwave.core.assembler.StaticComponentContainer;
import org.junit.BeforeClass;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.utils.Utils;

public class TaskSchedulerCommon {

	@BeforeClass
	public static void init() throws Exception {		
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
		StaticComponentContainer.Modules.exportAllToAll();
	}

	protected TaskSchedulerCommon() {
	}


}
