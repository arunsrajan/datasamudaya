package com.github.datasamudaya.common;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.nio.ByteBuffer;
import java.util.concurrent.atomic.AtomicInteger;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.misc.Unsafe;

import static java.util.Objects.nonNull;

/**
 * Utility which frees the Direct Byte Buffer memory
 * 
 * @author arun
 *
 */
public final class DirectByteBufferUtil {

	static Logger log = LoggerFactory.getLogger(DirectByteBufferUtil.class);

	static AtomicInteger allocation = new AtomicInteger(1);
	static AtomicInteger deallocation = new AtomicInteger(1);

	protected static Unsafe getUnsafe() {
		try {
			Field f = Unsafe.class.getDeclaredField("theUnsafe");
			f.setAccessible(true);
			return (Unsafe) f.get(null);
		} catch (Exception ex) {
			return null;
		}
	}

	/**
	 * Frees the specified buffer's direct memory allocation.<br>
	 * The buffer should not be used after calling this method; you should instead
	 * allow it to be garbage-collected by removing all references of it from your
	 * program.
	 * 
	 * @param directBuffer The direct buffer whose memory allocation will be freed
	 * @return Whether or not the memory allocation was freed
	 */
	public static synchronized boolean freeDirectBufferMemory(ByteBuffer buffer) {
		if (buffer != null) {
			if (!buffer.isDirect()) {
				return false;
			}
			try {
				Method cleanerMethod = buffer.getClass().getMethod("cleaner");
				cleanerMethod.setAccessible(true);
				Object cleaner = cleanerMethod.invoke(buffer);
				if (nonNull(cleaner)) {
					Method cleanMethod = cleaner.getClass().getMethod("clean");
					cleanMethod.setAccessible(true);
					cleanMethod.invoke(cleaner);
					log.info("Direct Byte Buffer recovering to pool exhaustion number {} with buffer info {}",
							deallocation.incrementAndGet(), buffer);
				}
				return true;
			} catch (Exception ex) {
				ex.printStackTrace();
				return false;
			}
		}
		return false;
	}

	/**
	 * Allocate direct memory for the given capacity.
	 * @param cap
	 * @return direct bytebuffer object
	 * @throws Exception
	 */
	public static synchronized ByteBuffer allocateDirect(int cap) throws Exception {
		ByteBuffer buffer = ByteBuffer.allocateDirect(cap);
		log.info("Direct Byte Buffer quota number {} with object info {}", allocation.incrementAndGet(), buffer);
		return buffer;
	}

	private DirectByteBufferUtil() {
	}

}
