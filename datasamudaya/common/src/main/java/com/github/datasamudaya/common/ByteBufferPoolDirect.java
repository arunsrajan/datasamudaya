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
import java.util.Collections;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Semaphore;

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

	private static List<ByteBuffer> bufferPool;
	static int totalnumberofblocks;
	static long directmemory;
	static long totalmemoryallocated;
	static Semaphore lock = new Semaphore(1);
	/**
	 * Initialize the bytebuffer heapsize and direct memory size
	 */
	public static void init(long dm) {		
		directmemory = dm;
		bufferPool = new Vector<>((int) (directmemory / (130 * DataSamudayaConstants.MB)) + 30);
        log.info("Total Buffer pool size: {}", bufferPool.size());
	}

	/**
	 * Get the direct ByteBuffer object with capacity as memory to allocate.
	 * @param memorytoallocate
	 * @return bytebuffer object
	 * @throws Exception
	 */
	public static synchronized ByteBuffer get(long memorytoallocate) throws Exception {
		lock.acquire();
		if(memorytoallocate+totalmemoryallocated<directmemory) {
			ByteBuffer bb = ByteBuffer.allocateDirect((int) memorytoallocate);
			totalmemoryallocated += memorytoallocate;
			lock.release();
			return bb;
		}
		Collections.sort(bufferPool, (bb1, bb2) -> {
			if (bb1.capacity() > bb2.capacity()) {
				return 1;
			} else if (bb1.capacity() < bb2.capacity()) {
				return -1;
			}
			return 0;
		});
		int bbindex=bufferPool.size()-1;
		for (;bbindex >= 0 && bufferPool.get(bbindex).capacity() >= memorytoallocate;bbindex--) {
		}
		if(bbindex == bufferPool.size()-1) {
			ByteBuffer bb = ByteBuffer.allocate((int) memorytoallocate);
			lock.release();
			return bb;
		}
		ByteBuffer bb = bufferPool.remove(bbindex + 1);
		bb.clear();
		bb.limit((int) memorytoallocate);
		lock.release();
		return bb;
	}

	public static void destroy() {
	}

	/**
	 * Unallocate direct byte buffer to reuse the memory 
	 * @param bb
	 * @throws Exception
	 */
	public static synchronized void destroy(ByteBuffer buffer) throws Exception {
		lock.acquire();
		buffer.clear();
		bufferPool.add(buffer);
		lock.release();
	}

	private ByteBufferPoolDirect() {
	}
}
