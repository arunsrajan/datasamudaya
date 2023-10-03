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

import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author arun
 * This class holds the information of users share 
 * of containers for launching the task executors. The key
 * is username and value is the user share in percentage
 */
public class DataSamudayaUsers {
	private DataSamudayaUsers() {
	}
	private static ConcurrentMap<String, User> users;

	public static void put(ConcurrentMap<String, User> users) {
		DataSamudayaUsers.users = users;
	}

	public static ConcurrentMap<String, User> get() {
		return DataSamudayaUsers.users;
	}
}
