package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class evaluates degrees
 */
public class DegreesUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);		
		return Double.valueOf(String.valueOf(value)) / Math.PI * 180;
	}

}
