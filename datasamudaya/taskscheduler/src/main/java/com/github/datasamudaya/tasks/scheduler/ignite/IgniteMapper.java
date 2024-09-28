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
package com.github.datasamudaya.tasks.scheduler.ignite;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.List;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;
import org.xerial.snappy.SnappyInputStream;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.tasks.executor.Mapper;

@SuppressWarnings("rawtypes")
public class IgniteMapper {
	@IgniteInstanceResource
	Ignite ignite;

	public IgniteMapper() {
	}

	static Logger log = Logger.getLogger(IgniteMapper.class);
	BlocksLocation blockslocation;
	List<Mapper> crunchmappers;
	byte[] mapperbytes;
	public IgniteMapper(BlocksLocation blockslocation, byte[] mapperbytes) {
		this.blockslocation = blockslocation;
		this.mapperbytes = mapperbytes;
	}

	public Context execute() throws Exception {
		try (IgniteCache<Object, byte[]> cache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
				var compstream = new SnappyInputStream(new ByteArrayInputStream(cache.get(Utils.getBlocksLocation(blockslocation))));
				var br =
						new BufferedReader(new InputStreamReader(compstream));) {
			var ctx = new DataCruncherContext();
			br.lines().parallel().forEachOrdered(line -> {
				for (Mapper crunchmapper : crunchmappers) {
					crunchmapper.map(0l, line, ctx);
				}
			});
			return ctx;
		}
		catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
			throw ex;
		}

	}


}
