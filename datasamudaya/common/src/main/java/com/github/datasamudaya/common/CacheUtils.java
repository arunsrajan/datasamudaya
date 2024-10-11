/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.io.InputStream;
import java.util.Objects;

import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.ehcache.CacheManager;
import org.ehcache.config.builders.CacheConfigurationBuilder;
import org.ehcache.config.builders.CacheManagerBuilder;
import org.ehcache.config.builders.ResourcePoolsBuilder;
import org.ehcache.config.units.MemoryUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.isNull;

/**
 * 
 * @author arun The cache helper class to initialize and build in memory cache.
 */
public class CacheUtils {

	static Logger log = LoggerFactory.getLogger(CacheUtils.class);

	private CacheUtils() {
	}

	public enum CacheExpiry {
		HOURS, MINUTES, SECONDS
	}

	/**
	* This functions builds cache objects given cachename, expiry and dizksize.
	* 
	* @param cachename
	* @param keytype
	* @param valuetype
	* @param sizeingb
	* @param expiry
	* @param cacheexpiry
	* @param disksizeingb
	* @param cachedatapath
	* @return cache object
	*/
	public static Cache<?, ?> buildInMemoryCache(String cachename, Class<?> keytype,
			Class<?> valuetype, int numbuffsize, int expiry, CacheExpiry cacheexpiry, int disksizeingb,
			String cachedatapath) {
		log.debug("Entered CacheUtils.buildInMemoryCache");
		CacheManager cacheManager;
		if (Objects.isNull(DataSamudayaCacheManager.get())) {
			cacheManager = CacheManagerBuilder.newCacheManagerBuilder()
					.with(CacheManagerBuilder.persistence(cachedatapath)).build();
			cacheManager.init();
			log.debug("Cache Manager Object Built");
			DataSamudayaCacheManager.put(cacheManager);
		} else {
			cacheManager = DataSamudayaCacheManager.get();
		}
		try {
			Cache<?, ?> cache = cacheManager.getCache(cachename, keytype, valuetype);
			if (isNull(cache)) {
				cache = createCache(cacheManager, cachename, keytype, valuetype, numbuffsize, disksizeingb);
			}
			return cache;
		} catch (Exception ex) {
			log.debug("Cache {} is not available, so creating cache... ", cachename);
			return createCache(cacheManager, cachename, keytype, valuetype, numbuffsize, disksizeingb);
		}
	}

	private static Cache<?, ?> createCache(CacheManager cacheManager, String cachename, Class<?> keytype
		, Class<?> valuetype, long numbuffsize, int disksizeingb) {
		return cacheManager.createCache(cachename,
				CacheConfigurationBuilder.newCacheConfigurationBuilder(
						keytype, valuetype,
						ResourcePoolsBuilder.newResourcePoolsBuilder()
								.heap(numbuffsize)
								.disk(disksizeingb, MemoryUnit.GB, true))
						.build());
	}

	/**
	 * This function initializes cache for the entire JVM.
	 */
	public static void initCache(String cacheName, String diskpath) {
		log.debug("Entered CacheUtils.initCache");
		String cacheduration = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDURATION, DataSamudayaConstants.CACHEDURATION_DEFAULT);
		DataSamudayaCache
				.put(buildInMemoryCache(cacheName, String.class, byte[].class,
						Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEHEAPMAXENTRIES,
								DataSamudayaConstants.CACHEHEAPMAXENTRIES_DEFAULT)),
						Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEEXPIRY, DataSamudayaConstants.CACHEEXPIRY_DEFAULT)),
						CacheUtils.CacheExpiry.valueOf(cacheduration),
						Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKSIZEGB, DataSamudayaConstants.CACHEDISKSIZEGB_DEFAULT)),
						diskpath));
		log.debug("Exiting CacheUtils.initCache");
	}

	/**
	* Initializes Block metadata cache
	*/
	public static void initBlockMetadataCache(String cache) {
		log.debug("Entered CacheUtils.initBlockMetadataCache");
		initFileBlocksMetadataCache();
		if (DataSamudayaCacheManager.get() != null) {
			DataSamudayaCacheManager.get().close();
			DataSamudayaCacheManager.put(null);
		}
		initFileBlocksMetadataCache();
		initCache(cache, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
				DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.CACHEBLOCKS);
		log.debug("Exiting CacheUtils.initBlockMetadataCache");
	}

	/**
	* The method which initializes both block and file metadata
	*/
	@SuppressWarnings("unchecked")
	private static void initFileBlocksMetadataCache() {
		String cacheduration = (String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEDURATION);
		DataSamudayaCache.putBlocksMetadata((Cache<String, BlocksLocation[]>) buildInMemoryCache(
				DataSamudayaConstants.BLOCKSLOCATIONMETADATACACHE, String.class, BlocksLocation[].class,
				Integer.parseInt((String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEHEAPMAXENTRIES,
						DataSamudayaConstants.CACHEHEAPMAXENTRIES_DEFAULT)),
				Integer.parseInt((String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEEXPIRY)),
				CacheUtils.CacheExpiry.valueOf(cacheduration),
				Integer.parseInt((String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEDISKSIZEGB)),
				(String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEBLOCKSLOCATIONDISKPATH,
						DataSamudayaConstants.CACHEBLOCKSLOCATIONDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.CACHEBLOCKS));
		DataSamudayaCache
				.putFileMetadata((Cache<String, String>) buildInMemoryCache(DataSamudayaConstants.FILEMETADATACACHE,
						String.class, String.class,
						Integer.parseInt((String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEHEAPMAXENTRIES,
								DataSamudayaConstants.CACHEHEAPMAXENTRIES_DEFAULT)),
						Integer.parseInt((String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEEXPIRY)),
						CacheUtils.CacheExpiry.valueOf(cacheduration),
						Integer.parseInt((String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEDISKSIZEGB)),
						(String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEFILEMETDATADISKPATH,
								DataSamudayaConstants.CACHEFILEMETDATADISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
								+ DataSamudayaConstants.CACHEFILE));
	}

	/**
	* This function returns block data in bytes in compressed stream using LZF compression.
	* 
	* @param blockslocation
	* @param hdfs
	* @return compressed stream object.
	* @throws Exception
	*/
	public static InputStream getBlockData(BlocksLocation blockslocation, FileSystem hdfs)
			throws Exception {
		log.debug("Entered CacheUtils.getBlockData");
		return HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);

	}
}
