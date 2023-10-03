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

import static org.junit.Assert.assertNotNull;
import java.io.InputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;

public class CacheUtilsTest {

	String hdfsurl = "hdfs://127.0.0.1:9000";
	String[] hdfsdirpaths = {"/airlines"};

	@BeforeClass
	public static void initCache() throws Exception {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_TEST_PROPERTIES);
		CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
	            + DataSamudayaConstants.CACHEBLOCKS);
		ByteBufferPoolDirect.init(2*DataSamudayaConstants.GB);
	}

	@Test
	public void testCache() throws Exception {
		FileSystem hdfs = FileSystem.get(new URI(hdfsurl), new Configuration());
		List<Path> blockpath  = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths) {
			FileStatus[] fileStatus = hdfs.listStatus(
					new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			blockpath.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, true, 128 * DataSamudayaConstants.MB, null);
		assertNotNull(bls);
		getDnXref(bls);
		String cacheblock = "cacheblock";
		int blscount = 0;
		Cache<String, byte[]> cache = (Cache<String, byte[]>) DataSamudayaCache.get();
		for (BlocksLocation bl :bls) {
			InputStream sis = HdfsBlockReader.getBlockDataInputStream(bl, hdfs);
			byte[] byt = sis.readAllBytes();
			cache.put(cacheblock + blscount, byt);
			blscount++;
			System.out.println(blscount);
			sis.close();
		}
	}

	public void getDnXref(List<BlocksLocation> bls) {

		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if(bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
				for (; xref.hasNext(); ) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));

		for (var b : bls) {
			var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
					.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
				return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
			}).findFirst();
			var xref = xrefselected.get();
			dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
			b.getBlock()[0].setHp(xref);
			if(b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
				xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
						.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				xref = xrefselected.get();
				b.getBlock()[1].setHp(xref);
			}
		}
	}

	@AfterClass
	public static void destroyCache() throws Exception {
		DataSamudayaCache.get().clear();
		DataSamudayaCacheManager.get().close();
		ByteBufferPoolDirect.destroy();
	}

}
