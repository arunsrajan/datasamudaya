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

import java.util.Objects;

@FunctionalInterface
public interface JoinPredicate<I1, I2> extends BiPredicateSerializable<I1, I2> {
	default JoinPredicate<I1, I2> and(JoinPredicate<I1, I2> other) {
		Objects.requireNonNull(other);
		return (I1 t1, I2 t2) -> test(t1, t2) && other.test(t1, t2);
	}

	default JoinPredicate<I1, I2> or(JoinPredicate<I1, I2> other) {
		Objects.requireNonNull(other);
		return (I1 t1, I2 t2) -> test(t1, t2) || other.test(t1, t2);
	}
}
