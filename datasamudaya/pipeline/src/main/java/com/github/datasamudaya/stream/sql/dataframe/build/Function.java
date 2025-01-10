package com.github.datasamudaya.stream.sql.dataframe.build;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;

@Getter
@AllArgsConstructor
@ToString
public class Function {
	private String name;
	private String alias;
	private Object[] operands;
}
