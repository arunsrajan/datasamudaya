package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class CeilUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		if (value instanceof Double dv) {
			return Math.ceil(dv);
		} else if (value instanceof Long lv) {
			return Math.ceil(lv);
		} else if (value instanceof Float fv) {
			return Math.ceil(fv);
		} else if (value instanceof Integer iv) {
			return Math.ceil(iv);
		}
		return value;
	}
}