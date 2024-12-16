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
package com.github.datasamudaya.tasks.yarn.executor;

import java.util.List;
import java.util.concurrent.Callable;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.tasks.executor.Reducer;

/**
 * Executor for the reducer.
 * 
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class ReducerExecutor implements Callable<Context> {

	static Logger log = LogManager.getLogger(ReducerExecutor.class);
	DataCruncherContext dcc;
	Reducer cr;
	Object key;

	public ReducerExecutor(DataCruncherContext dcc, Reducer cr, Object key) {
		this.dcc = dcc;
		this.cr = cr;
		this.key = key;
	}

	/**
	 * Executes the call method and returns context object.
	 */
	@SuppressWarnings({ "unchecked" })
	@Override
	public Context call() throws Exception {
		var ctx = new DataCruncherContext();
		dcc.keys().parallelStream().forEachOrdered(key -> {
			cr.reduce(key, (List) dcc.get(key), ctx);
		});
		return ctx;
	}

}
