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
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author Arun
 * File Split Block Information
 */
@Getter
@Setter
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Block implements Serializable, Cloneable {
	@Override
	public int hashCode() {
		return Objects.hash(blockOffset, blockend, blockstart, filename);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		Block other = (Block) obj;
		return blockOffset == other.blockOffset && blockend == other.blockend && blockstart == other.blockstart
				&& Objects.equals(filename, other.filename);
	}
	private static final long serialVersionUID = 1641172215309142006L;
	private long blockOffset;
	private long blockstart;
	private long blockend;
	private String filename;
	private String hp;
	private Map<String, Set<String>> dnxref;

	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
	
}
