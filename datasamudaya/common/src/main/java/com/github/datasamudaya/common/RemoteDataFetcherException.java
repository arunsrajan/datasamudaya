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

/**
 * 
 * @author arun
 * This class is exception class thrown when the RemoteDataFecth is executed
 * and also holds the error messages. 
 */
public class RemoteDataFetcherException extends Exception {

	private static final long serialVersionUID = 1121047245478192027L;

	public RemoteDataFetcherException(String message) {
		super(message);
	}

	RemoteDataFetcherException(String message, Exception ex) {
		super(message, ex);
	}

	static final String INTERMEDIATEPHASEWRITEERROR = "Write Intermediate Phase Output to DFS";
	static final String INTERMEDIATEPHASEREADERROR = "Read Intermediate Phase Output from DFS";
	static final String INTERMEDIATEPHASEDELETEERROR = "Delete Intermediate Phase Output from DFS";

}
