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

import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.List;
import java.util.concurrent.Callable;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.tasks.executor.Mapper;

/**
 * Executor for mapper.
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class MapperExecutor implements Callable<Context> {
	static Logger log = Logger.getLogger(MapperExecutor.class);
	BlocksLocation blockslocation;
	List<Mapper> crunchmappers;
	InputStream datastream;

	public MapperExecutor(BlocksLocation blockslocation, InputStream datastream, List<Mapper> crunchmappers) {
		this.blockslocation = blockslocation;
		this.datastream = datastream;
		this.crunchmappers = crunchmappers;
	}

	/**
	 * Executes the call method and returns context object.
	 */
	@Override
	public Context call() throws Exception {
		try (var compstream = datastream;
				var br =
						new BufferedReader(new InputStreamReader(compstream));) {
			var ctx = new DataCruncherContext();
			br.lines().parallel().forEachOrdered(line -> {
				for (var crunchmapper : crunchmappers) {
					crunchmapper.map(0l, line, ctx);
				}
			});
			return ctx;
		}
		catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
			throw ex;
		}

	}


}
