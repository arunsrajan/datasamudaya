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
package com.github.datasamudaya.tasks.scheduler;

/**
 * Exception message class for map reduce application.
 * @author arun
 *
 */
public class MapReduceException extends Exception {

	private static final long serialVersionUID = -7937152190279453002L;

	public static final String MEMORYALLOCATIONERROR = "Memory allocation error Minimum Required Memory is 128 MB";
	public static final String INSUFFMEMORYALLOCATIONERROR = "Insufficient memory, required memory is greater than the available memory";
	public static final String INSUFFNODESERROR = "Insufficient computing nodes";

	public MapReduceException(String message) {
		super(message);
	}
}
