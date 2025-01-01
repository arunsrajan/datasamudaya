package com.github.datasamudaya.stream.pig.udf;

import lombok.Getter;

/**
 * The UDF class evaluates COUNT
 */
@Getter
public class CountUDF extends org.apache.pig.builtin.COUNT implements EvalFuncName {

	String name;
	public CountUDF(String name) {
		this.name = name;
	}
}
