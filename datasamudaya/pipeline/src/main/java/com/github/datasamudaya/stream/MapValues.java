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
package com.github.datasamudaya.stream;

import org.jooq.lambda.tuple.Tuple2;

import com.github.datasamudaya.common.functions.MapValuesFunction;
import com.github.datasamudaya.common.functions.ReduceByKeyFunctionValues;

/**
 * 
 * @author arun
 * This class holds information for MapValues function.
 * @param <I1>
 * @param <I2>
 */
public final class MapValues<I1, I2> extends MapPair<I1, I2> {
	private MapValues(AbstractPipeline root,
			ReduceByKeyFunctionValues<I2> rfv)  {
		this.task = rfv;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * Constructor for MapValues
	 * @param <I3>
	 * @param <I4>
	 * @param root
	 * @param mvf
	 */
	public <I3, I4> MapValues(AbstractPipeline root, MapValuesFunction<? super I2, ? extends Tuple2<I3, I4>> mvf) {
		this.task = mvf;
		this.root = root;
		root.finaltask = task;
	}

	/**
	 * MapValues accepts ReduceFunctionValues.
	 * @param rfv
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public MapValues<I1, I2> reduceByValues(ReduceByKeyFunctionValues<I2> rfv)  {
		var mapvalues = new MapValues(root, rfv);
		this.childs.add(mapvalues);
		mapvalues.parents.add(this);
		return mapvalues;
	}
}
