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
import java.util.Collection;
import java.util.Hashtable;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.UUID;
import java.util.Vector;
import java.util.concurrent.atomic.AtomicLong;

import lombok.EqualsAndHashCode;
import lombok.ToString;

/**
 * 
 * @author arun
 * The implemention class for storing the key value pairs in MR api.
 * @param <K>
 * @param <V>
 */
@EqualsAndHashCode
@ToString
public class DataCruncherContext<K, V> implements Context<K, V>, Serializable {

	private String contextid = UUID.randomUUID().toString();
	
	private Map<K, Collection<V>> htkv = new Hashtable<>();

	private AtomicLong valuescounter = new AtomicLong(0);
	
	@Override
	public void put(K k, V v) {
		if (htkv.get(k) == null) {
			htkv.put(k, new Vector<>());
		}
		htkv.get(k).add(v);
		valuescounter.incrementAndGet();
	}

	@Override
	public Collection<V> get(K k) {
		return htkv.get(k);
	}

	@Override
	public Set<K> keys() {
		return htkv.keySet();
	}

	@Override
	public void addAll(K k, Collection<V> v) {
		if (htkv.get(k) != null) {
			htkv.get(k).addAll(v);
		} else if (v == null) {
			htkv.put(k, new Vector<>());
		} else {
			htkv.put(k, v);
		}
		valuescounter.addAndGet(v.size());
	}

	@Override
	public void putAll(Set<K> k, V v) {
		k.stream().forEach(key -> {
			if (htkv.get(key) == null) {
				htkv.put(key, new LinkedHashSet<>());
			}
			put(key, v);
		});
	}

	@Override
	public void add(Context<K, V> ctx) {
		ctx.keys().stream().forEach(key -> addAll(key, ctx.get(key)));

	}

	public String getContextid() {
		return contextid;
	}
	
	@Override
	public long size() {
		return htkv.size();
	}
	
	@Override
	public long valuesSize() {
		return valuescounter.get();
	}

	@Override
	public void clear() {		
		htkv.clear();
	}

	@Override
	public Set<Entry<K, Collection<V>>> entries() {		
		return htkv.entrySet();
	}
	
}
