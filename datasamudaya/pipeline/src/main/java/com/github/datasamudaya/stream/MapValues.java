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
	protected MapValues(AbstractPipeline parent,
			Object task) {
		parent.childs.add(this);
		this.parents.add(parent);
		tasks.add(task);
	}

	/**
	 * MapValues accepts ReduceFunctionValues.
	 * @param rfv
	 * @return
	 */
	@SuppressWarnings({"unchecked", "rawtypes"})
	public MapValues<I1, I2> reduceByValues(ReduceByKeyFunctionValues<I2> rfv) {
		var mapvalues = new MapValues(this, rfv);
		
		return mapvalues;
	}
}
