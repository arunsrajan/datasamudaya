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

import java.util.Set;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;

import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.RetrieveKeys;
import com.github.datasamudaya.common.TaskSchedulerMapperCombinerSubmitterMBean;

/**
 * Task scheduler for mapper combiner.
 * @author arun
 *
 */
public class TaskSchedulerMapperCombinerSubmitter extends TaskSchedulerMapperSubmitter
		implements TaskSchedulerMapperCombinerSubmitterMBean,Callable<RetrieveKeys> {
	Set<Object> combinerclasses;

	TaskSchedulerMapperCombinerSubmitter(Object blockslocation, boolean mapper,
			Set<Object> mapperclasses, Set<Object> combinerclasses,
			CuratorFramework cf,
			ApplicationTask apptask, String containerid) {
		super(blockslocation, mapper, mapperclasses, apptask, cf, containerid);
		this.combinerclasses = combinerclasses;
	}

	@Override
	public RetrieveKeys call() throws Exception {
		var blockslocation = initializeobject(mapperclasses, combinerclasses);
		return (RetrieveKeys) sendChunk(blockslocation);
	}

	@Override
	public long getCombinerSubmittedCount() {
		return 0;
	}

}
