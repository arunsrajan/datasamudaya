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

import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import lombok.Getter;

/**
 * This class holds the objects for the container allocation and deallocation.
 * @author arun
 *
 */
public class GlobalContainerAllocDealloc {
	@Getter
	private static Map<String, List<String>> jobcontainerids = new ConcurrentHashMap<>();
	@Getter
	private static Semaphore globalcontainerallocdeallocsem = new Semaphore(1);
	@Getter
	private static Map<String, ContainerResources> hportcrs = new ConcurrentHashMap<>();
	@Getter
	private static Map<String, String> containernode = new ConcurrentHashMap<>();
	@Getter
	private static Map<String, Set<String>> nodecontainers = new ConcurrentHashMap<>();

	private GlobalContainerAllocDealloc() {
	}


}
