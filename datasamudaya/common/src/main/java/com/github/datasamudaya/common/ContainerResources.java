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
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author arun
 * The required container parameters or resources information for launching single container. 
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@ToString
@EqualsAndHashCode
public class ContainerResources implements Serializable, Cloneable {
	private static final long serialVersionUID = 1492654050202561687L;
	private long minmemory;
	private long maxmemory;
	private long directheap;
	private String gctype;
	private int cpu;
	private int port;
	private boolean islaunched;
	private EXECUTORTYPE executortype;

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();

	}
}
