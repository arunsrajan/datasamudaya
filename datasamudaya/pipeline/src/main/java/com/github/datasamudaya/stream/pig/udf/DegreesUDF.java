package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class evaluates degrees
 */
@Getter
@AllArgsConstructor
public class DegreesUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);		
		return Double.valueOf(String.valueOf(value)) / Math.PI * 180;
	}

}
