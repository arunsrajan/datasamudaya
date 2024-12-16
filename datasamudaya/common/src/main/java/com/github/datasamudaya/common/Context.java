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

import java.util.Collection;
import java.util.Map.Entry;
import java.util.Set;

/**
 * 
 * @author Arun
 * The context interface for Map Reduce Framework Api
 * @param <K> Key
 * @param <V> Value
 */
public interface Context<K, V> {

	public void put(K k, V v);

	public void putAll(Set<K> k, V v);

	public Collection<V> get(K k);

	public Set<K> keys();

	public void addAll(K k, Collection<V> v);

	public void add(Context<K, V> ctx);

	public long size();

	public long valuesSize();

	public void clear();

	public Set<Entry<K, Collection<V>>> entries();
}
