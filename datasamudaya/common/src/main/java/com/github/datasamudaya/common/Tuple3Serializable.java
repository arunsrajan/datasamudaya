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

import java.io.Serializable;
import org.jooq.lambda.tuple.Tuple3;

/**
 * 
 * @author arun
 * This class is the derivied class of the Tuple3 with serializable.
 * @param <V1>
 * @param <V2>
 */
public class Tuple3Serializable<V1, V2, V3> extends Tuple3<V1, V2, V3> implements Serializable {

	private static final long serialVersionUID = -622259929441801409L;


	public Tuple3Serializable() {
		super(null, null, null);
	}


	public Tuple3Serializable(V1 v1, V2 v2, V3 v3) {
		super(v1, v2, v3);
	}

}
