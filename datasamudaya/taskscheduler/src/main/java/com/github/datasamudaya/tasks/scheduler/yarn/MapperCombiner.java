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
package com.github.datasamudaya.tasks.scheduler.yarn;

import java.util.Set;

import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.BlocksLocation;

public class MapperCombiner {
	BlocksLocation blockslocation;
	Set<Object> mapperclasses;
	ApplicationTask apptask;
	Set<Object> combinerclasses;

	public MapperCombiner(BlocksLocation blockslocation, Set<Object> mapperclasses, ApplicationTask apptask,
			Set<Object> combinerclasses) {
		this.blockslocation = blockslocation;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.combinerclasses = combinerclasses;
	}

	public BlocksLocation getBlockslocation() {
		return blockslocation;
	}

	public void setBlockslocation(BlocksLocation blockslocation) {
		this.blockslocation = blockslocation;
	}

	public Set<Object> getMapperclasses() {
		return mapperclasses;
	}

	public void setMapperclasses(Set<Object> mapperclasses) {
		this.mapperclasses = mapperclasses;
	}

	public ApplicationTask getApptask() {
		return apptask;
	}

	public void setApptask(ApplicationTask apptask) {
		this.apptask = apptask;
	}

	public Set<Object> getCombinerclasses() {
		return combinerclasses;
	}

	public void setCombinerclasses(Set<Object> combinerclasses) {
		this.combinerclasses = combinerclasses;
	}

	@Override
	public String toString() {
		return "MapperCombiner [blockslocation=" + blockslocation + ", mapperclasses=" + mapperclasses + ", apptask="
				+ apptask + ", combinerclasses=" + combinerclasses + "]";
	}


}
