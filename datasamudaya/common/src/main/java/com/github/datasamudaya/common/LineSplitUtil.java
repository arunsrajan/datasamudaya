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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;

/**
 * 
 * @author Arun
 * The tokenization of bytes to string
 */
public class LineSplitUtil {

	/**
	 * The calculation of line count by passing bytes.
	 * @param byt
	 * @return
	 * @throws Exception
	 */
	public static Long splitcount(byte[] byt) throws Exception {
		var count = 0l;
		for (var bt :byt) {
			if (bt == '\n') {
				count++;
			}
		}
		if (byt[byt.length - 1] != '\n') {
			count++;
		}
		return count;
	}

	/**
	 * Splitting of lines from bytes.
	 * @param byt
	 * @return
	 * @throws Exception
	 */
	public static String[] split(byte[] byt) throws Exception {
		var reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(byt)));
		String line;
		var strings = new ArrayList<>();
		while ((line = getNextLine(reader)) != null) {
			strings.add(line);
		}
		reader.close();
		return strings.toArray(new String[strings.size()]);
	}

	public static String getNextLine(BufferedReader reader) throws Exception {
		return reader.readLine();
	}

	private LineSplitUtil() {
	}
}
