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
import java.util.List;
import java.util.Map;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;

public class CarriersDataMapper implements Mapper<Long, String, Context<String, Map>>,
		Reducer<String, Map, Context<String, String>>, Combiner<String, Map, Context<String, Map>> {
	private static final long serialVersionUID = -1305436970635920292L;
	public static final String CARRIERS = "CARRIERS";
	public static final String ARRIVALDELAY = "AIRLINEARRDELAY";
	public static final String AIRLINEDEPDELAY = "AIRLINEDEPDELAY";

	@Override
	public void map(Long chunkid, String line, Context<String, Map> ctx) {
		var contents = line.split(",");
		if (contents[0] != null && !"Code".equals(contents[0])) {
			if (contents != null && contents.length > 1) {
				var map = new HashMap<String, String>();
				map.put(CARRIERS, contents[1].substring(1, contents[1].length() - 1));
				ctx.put(contents[0].substring(1, contents[0].length() - 1), map);
			}
		}
	}

	@Override
	public void reduce(String key, List<Map> values, Context<String, String> context) {
		var arrdelaysum = 0l;
		var depdelaysum = 0l;
		var carrierName = "";
		for (var map : values) {
			if (map.get(CARRIERS) != null) {
				carrierName = (String) map.get(CARRIERS);
			}
			if (map.get(ARRIVALDELAY) != null) {
				arrdelaysum += (long) map.get(ARRIVALDELAY);
			}
			if (map.get(AIRLINEDEPDELAY) != null) {
				depdelaysum += (long) map.get(AIRLINEDEPDELAY);
			}
		}
		var map1 = new HashMap<>();
		if (!(arrdelaysum == 0 && depdelaysum == 0)) {
			context.put(key, carrierName + "," + (arrdelaysum == 0 ? "" : arrdelaysum) + ","
					+ (depdelaysum == 0 ? "" : depdelaysum));
		}

	}

	@Override
	public void combine(String key, List<Map> values, Context<String, Map> context) {
		var arrdelaysum = 0l;
		var depdelaysum = 0l;
		var mapdelaywithcarrier = new HashMap<>();
		for (var map : values) {
			if (map.get(CARRIERS) != null) {
				mapdelaywithcarrier.put(CARRIERS, map.get(CARRIERS));
			}
			if (map.get(ARRIVALDELAY) != null) {
				arrdelaysum += (long) map.get(ARRIVALDELAY);
			}
			if (map.get(AIRLINEDEPDELAY) != null) {
				depdelaysum += (long) map.get(AIRLINEDEPDELAY);
			}
		}

		mapdelaywithcarrier.put(ARRIVALDELAY, arrdelaysum);
		mapdelaywithcarrier.put(AIRLINEDEPDELAY, depdelaysum);
		context.put(key, mapdelaywithcarrier);
	}

}
