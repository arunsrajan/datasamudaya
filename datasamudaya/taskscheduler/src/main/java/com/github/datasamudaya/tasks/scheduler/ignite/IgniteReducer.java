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

import java.io.ByteArrayInputStream;
import java.util.List;

import org.apache.ignite.lang.IgniteCallable;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.tasks.executor.Reducer;

@SuppressWarnings("rawtypes")
public class IgniteReducer implements IgniteCallable<Context> {

	private static final long serialVersionUID = -1246953663442464999L;
	static Logger log = LogManager.getLogger(IgniteReducer.class);
	DataCruncherContext dcc;
	Reducer cr;
	byte[] redbytes;

	public IgniteReducer(DataCruncherContext dcc, byte[] redbytes) {
		this.dcc = dcc;
		this.redbytes = redbytes;
	}

	@SuppressWarnings({ "unchecked" })
	@Override
	public Context call() throws Exception {
		Kryo kryo = Utils.getKryoInstance();
		try (var istream = new ByteArrayInputStream(redbytes); var input = new Input(istream)) {
			this.cr = (Reducer) kryo.readClassAndObject(input);
		} catch (Exception ex) {

		}
		var ctx = new DataCruncherContext();
		dcc.keys().parallelStream().forEachOrdered(key -> {
			cr.reduce(key, (List) dcc.get(key), ctx);
		});
		return ctx;
	}

}
