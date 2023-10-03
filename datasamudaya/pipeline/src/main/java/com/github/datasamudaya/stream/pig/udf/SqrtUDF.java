package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class SqrtUDF extends EvalFunc<Object> {

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