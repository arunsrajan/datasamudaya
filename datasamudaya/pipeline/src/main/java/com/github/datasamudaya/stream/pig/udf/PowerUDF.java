package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PowerUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		Object powerval = (Object) input.get(1);
		if (value instanceof Double dv && powerval instanceof Integer powval) {
			return Math.pow(dv, powval);
		} else if (value instanceof Long lv && powerval instanceof Integer powval) {
			return Math.pow(lv, powval);
		} else if (value instanceof Float fv && powerval instanceof Integer powval) {
			return Math.pow(fv, powval);
		} else if (value instanceof Integer iv && powerval instanceof Integer powval) {
			return Math.pow(iv, powval);
		}
		return value;
	}
}