package com.github.datasamudaya.stream.pig.udf;

import lombok.Getter;

/**
 * The UDF class returns the sum of all the arguments
 */
@Getter
public class SumUDF extends org.apache.pig.builtin.SUM implements EvalFuncName {

	String name;
	public SumUDF(String name) {
		this.name = name;
	}
}
