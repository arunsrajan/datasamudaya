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
package com.github.datasamudaya.tasks.executor;

import java.util.List;
import java.util.Set;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingContext;

/**
 * Executor for the reducer.
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class ReducerExecutor implements Callable<Context> {

	static Logger log = Logger.getLogger(ReducerExecutor.class);
	Context dcc;
	Reducer cr;
	Object key;
	Task task;
	public ReducerExecutor(Context dcc, Reducer cr, Object key, Task task) {
		this.dcc = dcc;
		this.cr = cr;
		this.key = key;
		this.task = task;
	}

	/**
	 * Executes the call method and returns context object.
	 */
	@SuppressWarnings({"unchecked"})
	@Override
	public Context call() throws Exception {
		Set<Object> keys = dcc.keys();
		var ctx = new DiskSpillingContext(task, DataSamudayaConstants.EMPTY+System.currentTimeMillis());
		keys.stream().forEachOrdered(key -> {
			cr.reduce(key, (List) dcc.get(key), ctx);
		});
		return ctx;
	}

}
