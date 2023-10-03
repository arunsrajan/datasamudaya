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
package com.github.datasamudaya.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Global job metrics inmemory store
 * @author arun
 *
 */
public class DataSamudayaJobMetrics {
	private DataSamudayaJobMetrics() {
	}
	static Map<String, JobMetrics> jms = new ConcurrentHashMap<String, JobMetrics>();

	public static void put(JobMetrics jm) {
		jms.put(jm.getJobid(), jm);
	}

	public static Map<String, JobMetrics> get() {
		return jms;
	}

	public static JobMetrics get(String id) {
		return jms.get(id);
	}

}
