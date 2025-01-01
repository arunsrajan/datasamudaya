package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class evaluates PI
 */
@Getter
@AllArgsConstructor
public class PiiUDF extends EvalFunc<Object> implements EvalFuncName {
	String name;
	@Override
	public Object exec(Tuple input) throws IOException {		
		return Math.PI;
	}

}
