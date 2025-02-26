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
package com.github.datasamudaya.tasks.scheduler.ignite;

import java.util.List;
import java.util.Set;
import org.apache.ignite.lang.IgniteCallable;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.tasks.executor.Combiner;

@SuppressWarnings("rawtypes")
public class IgniteCombiner implements IgniteCallable<Context> {
	private static final long serialVersionUID = -2886619127389224706L;

	static Logger log = Logger.getLogger(IgniteCombiner.class);
	Context dcc;
	Combiner cc;

	public IgniteCombiner(Context dcc, Combiner cc) {
		this.dcc = dcc;
		this.cc = cc;
	}

	@SuppressWarnings("unchecked")
	@Override
	public Context call() throws Exception {
		Set<Object> keys = dcc.keys();
		var ctx = new DataCruncherContext();
		keys.stream().parallel().forEach(key -> {
			cc.combine(key, (List) dcc.get(key), ctx);

		});
		return ctx;
	}

}
