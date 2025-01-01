package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class returns the square root of the given value
 */
@Getter
@AllArgsConstructor
public class SqrtUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		if (value instanceof Double dv) {
			return Math.sqrt(dv);
		} else if (value instanceof Long lv) {
			return Math.sqrt(lv);
		} else if (value instanceof Float fv) {
			return Math.sqrt(fv);
		} else if (value instanceof Integer iv) {
			return Math.sqrt(iv);
		}
		return value;
	}
}
