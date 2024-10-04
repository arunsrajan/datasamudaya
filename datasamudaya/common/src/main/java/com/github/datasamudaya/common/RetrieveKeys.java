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

import java.io.Serializable;
import java.util.Set;

import lombok.ToString;

/**
 * 
 * @author arun
 * This class holds the data storing the keys in the HDFS for
 *  the MR Job APi.
 */
@ToString
public class RetrieveKeys implements Serializable {

	private static final long serialVersionUID = -100582342914768069L;

	public String applicationid;
	public String stageid;
	public String taskid;
	public Set<Object> keys;
	public boolean response;
}
