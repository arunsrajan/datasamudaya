package com.github.datasamudaya.common;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Random;
import java.util.Vector;

import org.burningwave.core.assembler.StaticComponentContainer;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.utils.Utils;

public class ByteBufferPoolDirectTest {
	static Logger log = LoggerFactory.getLogger(ByteBufferPoolDirectTest.class);
	@BeforeClass
	public static void initCache() throws Exception {
		StaticComponentContainer.Modules.exportAllToAll();
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_TEST_PROPERTIES);
		CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
	            + DataSamudayaConstants.CACHEBLOCKS);
		ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
	}
	@Test
	public void testByteBufferPool() throws Exception {
		int numiteration = 1000;
		int count = 0;
		Random rand = new Random(System.currentTimeMillis());
		List<Thread> threads = new Vector<>();
		int processors = Runtime.getRuntime().availableProcessors();
		while(count<numiteration) {
			Thread thr = new Thread(() -> {
				ByteBuffer bf = null;
			try {
				bf = ByteBufferPoolDirect.get(128 * 1024 * 1024);
				log.info("" + bf + " is Direct: " + bf.isDirect());
				Thread.sleep(rand.nextLong(10000));
			} catch (Exception e) {
				e.printStackTrace();
			}
			finally {
				try {
					log.info("Destroying Byte Buffer:" + bf);
					ByteBufferPoolDirect.destroy(bf);
					log.info("Destroyed Byte Buffer:" + bf);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			});
			threads.add(thr);
			thr.start();
			count++;
			if(count%processors==0) {
				Thread.sleep(1000);
			}
		}
		while(true) {
			if (threads.isEmpty()) {
				break;
			}
			Thread thr = threads.remove(0);
			thr.join();
		}
	}
	@AfterClass
	public static void destroyCache() throws Exception {
		DataSamudayaCache.get().clear();
		DataSamudayaCacheManager.get().close();
		ByteBufferPoolDirect.destroy();
	}
}
