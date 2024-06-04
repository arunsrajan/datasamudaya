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
package com.github.datasamudaya.common.functions;

public class FoldByKey {
	private  @SuppressWarnings("rawtypes")
	ReduceByKeyFunction reduceFunction;
	Object value;
	boolean left;

	@SuppressWarnings("rawtypes")
	public FoldByKey(Object value, ReduceByKeyFunction reduceFunction, boolean left) {
		this.reduceFunction = reduceFunction;
		this.value = value;
		this.left = left;
	}

	@SuppressWarnings("rawtypes")
	public ReduceByKeyFunction getReduceFunction() {
		return reduceFunction;
	}

	@SuppressWarnings("rawtypes")
	public void setReduceFunction(ReduceByKeyFunction reduceFunction) {
		this.reduceFunction = reduceFunction;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public boolean isLeft() {
		return left;
	}

	public void setLeft(boolean left) {
		this.left = left;
	}

}
