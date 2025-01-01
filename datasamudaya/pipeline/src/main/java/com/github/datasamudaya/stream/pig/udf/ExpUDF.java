package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class evaluates Exponential
 */
@Getter
@AllArgsConstructor
public class ExpUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		if (value instanceof Double dv) {
			return Math.exp(dv);
		} else if (value instanceof Long lv) {
			return Math.exp(lv);
		} else if (value instanceof Float fv) {
			return Math.exp(fv);
		} else if (value instanceof Integer iv) {
			return Math.exp(iv);
		}
		return value;
	}
}
