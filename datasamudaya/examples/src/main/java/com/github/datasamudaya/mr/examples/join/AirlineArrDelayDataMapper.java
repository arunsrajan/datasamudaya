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
package com.github.datasamudaya.mr.examples.join;

import java.util.HashMap;
import java.util.Map;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.tasks.executor.Mapper;

public class AirlineArrDelayDataMapper implements Mapper<Long, String, Context<String, Map<String, Long>>> {
	private static final long serialVersionUID = 2126665489375117940L;

	@Override
	public void map(Long chunkid, String line, Context<String, Map<String, Long>> ctx) {
		var contents = line.split(",");
		var map = new HashMap<String, Long>();

		if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14]) && !"ArrDelay".equals(contents[14])) {
			map.put("AIRLINEARRDELAY", Long.parseLong(contents[14]));
			ctx.put(contents[8], map);
		}

	}

}
