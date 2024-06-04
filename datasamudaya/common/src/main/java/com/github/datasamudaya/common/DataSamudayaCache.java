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

import org.ehcache.Cache;

/**
 * 
 * @author arun
 * This class holds the cache object for the whole JVM.
 */
public class DataSamudayaCache {
	private static Cache<?, ?> cache;
	private static Cache<String, BlocksLocation[]> blockslocationmetadatacache;
	private static Cache<String, String> filemetadatacache;

	public static void put(Cache<?, ?> cache) {
		DataSamudayaCache.cache = cache;
	}

	public static Cache<?, ?> get() {
		return DataSamudayaCache.cache;
	}

	public static void putBlocksMetadata(Cache<String, BlocksLocation[]> blockslocationmetadatacache) {
		DataSamudayaCache.blockslocationmetadatacache = blockslocationmetadatacache;
	}

	public static Cache<String, BlocksLocation[]> getBlocksMetadata() {
		return DataSamudayaCache.blockslocationmetadatacache;
	}

	public static void putFileMetadata(Cache<String, String> filemetadatacache) {
		DataSamudayaCache.filemetadatacache = filemetadatacache;
	}

	public static Cache<String, String> getFileMetadata() {
		return DataSamudayaCache.filemetadatacache;
	}


	private DataSamudayaCache() {
	}
}
