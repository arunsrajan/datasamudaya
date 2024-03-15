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

import java.io.Serializable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * 
 * @author arun
 * This class is options for CSV based pipeline.
 */
public class CsvOptions implements Serializable {
	private static final long serialVersionUID = -4005311056383759864L;

	public CsvOptions() {
		super();
	}
	private String[] header;
	private ConcurrentMap<String, Integer> headerindexmap = new ConcurrentHashMap<>();

	public CsvOptions(String[] header) {
		this.header = header;
		int count = 0;
		for (String head :header) {
			headerindexmap.put(head, count);
			count++;
		}
	}

	public String get(int headerindex) {
		return header[headerindex];
	}

	public int getIndexForHeader(String headerindex) {
		return headerindexmap.get(headerindex);
	}

	public String[] getHeader() {
		return header;
	}

	public ConcurrentMap<String, Integer> getHeaderindexmap() {
		return headerindexmap;
	}

}
