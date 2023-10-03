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

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;

@SuppressWarnings("rawtypes")
public class AirlineDataMapper implements Mapper<Long, String, Context>, Combiner<String, Long, Context>,
		Reducer<String, Long, Context> {

	@SuppressWarnings("unchecked")
	@Override
	public void combine(String key, List<Long> values, Context context) {
		long sum = 0;
		for (Long value :values) {
			sum += value;
		}
		context.put(key, sum);

	}

	@SuppressWarnings("unchecked")
	@Override
	public void map(Long index, String line, Context context) {

		String[] contents = line.split(",");
		if (contents[0] != null && !"Year".equals(contents[0])) {
			if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14])) {
				context.put(contents[8], Long.parseLong(contents[14]));
			}
		}


	}

	@Override
	public void reduce(String key, List<Long> values, Context context) {
		long sum = 0;
		for (Long value :values) {
			sum += value;
		}
		context.put(key, sum);

	}

}
