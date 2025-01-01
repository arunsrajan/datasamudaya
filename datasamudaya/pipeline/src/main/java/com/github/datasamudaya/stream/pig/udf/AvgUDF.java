package com.github.datasamudaya.stream.pig.udf;

import lombok.Getter;

/**
 * The UDF class evaluates AVG
 */
@Getter
public class AvgUDF extends org.apache.pig.builtin.SUM implements EvalFuncName {

	String name;
	public AvgUDF(String name) {
		this.name = name;
	}
}
