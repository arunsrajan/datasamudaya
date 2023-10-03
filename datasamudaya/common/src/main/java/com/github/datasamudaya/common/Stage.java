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
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 * @author Arun
 * Stage information of the streaming MR api.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Stage implements Serializable,Cloneable {

	private static final long serialVersionUID = 2272815602378403537L;
	public String id;
	public Integer number;
	public List<Object> tasks = new ArrayList<>();
	public Set<Stage> parent = new LinkedHashSet<>(),child = new LinkedHashSet<>();
	public Boolean isstagecompleted = false;
	public Boolean tovisit = true;

	@Override
	public Stage clone() throws CloneNotSupportedException {
		return (Stage) super.clone();
	}

	@Override
	public String toString() {
		return id;
	}

}
