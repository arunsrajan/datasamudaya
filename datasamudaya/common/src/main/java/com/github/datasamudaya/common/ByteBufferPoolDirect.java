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

import java.nio.ByteBuffer;

import org.apache.commons.pool2.impl.AbandonedConfig;
import org.apache.commons.pool2.impl.GenericObjectPool;
import org.apache.commons.pool2.impl.GenericObjectPoolConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * Direct Byte buffer pool which allocates byte buffer
 * 
 * @author arun
 *
 */
public class ByteBufferPoolDirect {

	private static final Logger log = LoggerFactory.getLogger(ByteBufferPoolDirect.class);

	private static transient GenericObjectPool<ByteBuffer> pool;
	static long directmemory;
	private static boolean initialized;

	/**
	 * Initialize the bytebuffer heapsize and direct memory size
	 */
	public static void init(long dm) {
		directmemory = dm;
		log.debug("Total memory In Bytes: {}", directmemory);
		GenericObjectPoolConfig config = new GenericObjectPoolConfig();
		config.setMaxIdle(DataSamudayaConstants.BYTEBUFFERPOOLMAXIDLE);
		config.setBlockWhenExhausted(false);
		config.setMaxTotal((int) (directmemory / DataSamudayaConstants.BYTEBUFFERSIZE));
		pool = new GenericObjectPool<>(new ByteBufferFactory(DataSamudayaConstants.BYTEBUFFERSIZE), config);
		var abandoned = new AbandonedConfig();
		abandoned.setRemoveAbandonedOnBorrow(false);
		pool.setAbandonedConfig(abandoned);
		initialized = true;
	}

	/**
	 * Get the direct ByteBuffer object with capacity as memory to allocate.
	 * @param memorytoallocate
	 * @return bytebuffer object
	 * @throws Exception
	 */
	public static ByteBuffer get() throws Exception {
		try {
			ByteBuffer bb = pool.borrowObject();
			bb.position(bb.limit());
			return bb;
		} finally {
		}
	}

	/**
	 * Destroys the Buffer Pool
	 */
	public static void destroyPool() {
		if (pool != null && !pool.isClosed()) {
			try {
				pool.close();
				pool = null;
			} catch (Exception e) {
				// Handle exception, possibly logging it
			}
		}
	}

	/**
	 * Unallocate direct byte buffer to reuse the memory 
	 * @param bb
	 * @throws Exception
	 */
	public static void destroy(ByteBuffer buffer) throws Exception {
		buffer.clear();
		pool.returnObject(buffer);
	}

	private ByteBufferPoolDirect() {
	}

	public static boolean isInitialized() {
		return initialized;
	}

}
