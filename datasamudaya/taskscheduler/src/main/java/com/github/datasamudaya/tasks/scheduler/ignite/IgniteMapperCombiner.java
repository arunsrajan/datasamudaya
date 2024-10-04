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

import java.io.ByteArrayInputStream;
import java.util.List;
import org.apache.ignite.lang.IgniteCallable;
import org.jgroups.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;

public class IgniteMapperCombiner extends IgniteMapper implements IgniteCallable<MapReduceResult> {
	private static final long serialVersionUID = -661033112077346441L;
	@SuppressWarnings("rawtypes")
	List<Combiner> crunchcombiners;
	byte[] combinerbytes;
	Logger log = LoggerFactory.getLogger(IgniteMapperCombiner.class);
	public IgniteMapperCombiner(BlocksLocation blockslocation, byte[] mapperbytes,
			byte[] combinerbytes) {		
		super(blockslocation, mapperbytes);		
		this.combinerbytes = combinerbytes;
	}

	@SuppressWarnings({"rawtypes"})
	@Override
	public MapReduceResult call() throws Exception {
		Kryo kryo = Utils.getKryoInstance();
		try(var baism = new ByteArrayInputStream(mapperbytes);
				Input inputm = new Input(baism);
				var baisc = new ByteArrayInputStream(combinerbytes);
				Input inputc = new Input(baisc);){
			this.crunchmappers = (List<Mapper>) kryo.readClassAndObject(inputm);		
			this.crunchcombiners = (List<Combiner>) kryo.readClassAndObject(inputc);
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		var starttime = System.currentTimeMillis();
		var ctx = super.execute();
		if (crunchcombiners != null && crunchcombiners.size() > 0) {
			var datasamudayac = new IgniteCombiner(ctx, crunchcombiners.get(0));
			ctx = datasamudayac.call();
		}
		var mrresult = new MapReduceResult();
		mrresult.cachekey = UUID.randomUUID().toString();
		var cache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHEMR);
		cache.put(mrresult.cachekey, (DataCruncherContext) ctx);
		var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
		log.info("Time taken to measure mapper task is " + timetaken + " seconds");
		return mrresult;
	}

	public BlocksLocation getBlocksLocation() {
		return this.blockslocation;
	}

}
