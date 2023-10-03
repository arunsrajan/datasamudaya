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

import java.io.Serializable;
import java.util.Objects;
import java.util.function.Predicate;

@FunctionalInterface
public interface PredicateSerializable<O> extends Predicate<O>, Serializable {
	default PredicateSerializable<O> and(PredicateSerializable<? super O> other) {
		Objects.requireNonNull(other);
		return t -> test(t) && other.test(t);
	}

	default PredicateSerializable<O> or(PredicateSerializable<? super O> other) {
		Objects.requireNonNull(other);
		return t -> test(t) || other.test(t);
	}
}
