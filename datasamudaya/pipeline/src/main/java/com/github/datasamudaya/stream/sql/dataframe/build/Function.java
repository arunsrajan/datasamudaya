package com.github.datasamudaya.stream.sql.dataframe.build;

import lombok.AllArgsConstructor;
import lombok.Getter;

@Getter
@AllArgsConstructor
public class Function {
	private String name;
	private String alias;
	private String expression;
}
