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
import java.util.ArrayList;
import java.util.List;

import org.apache.calcite.sql.type.SqlTypeName;

import lombok.Getter;
import lombok.Setter;

/**
 * 
 * @author arun
 * This class json based option for JSON based data pipeline.
 */
@Getter
@Setter
public class JsonSQL extends Json implements Serializable {
	private static final long serialVersionUID = 433665395278296254L;
	private String[] header;
	private List<SqlTypeName> types = new ArrayList<>();
	private List<String> requiredcolumns = new ArrayList<>();
	public JsonSQL(String[] header, List<SqlTypeName> types, List<String> requiredcolumns) {
		this.header = header;
		this.types = types;
		this.requiredcolumns = requiredcolumns;
	}
}
