package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.util.Base64;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class Base64DecodeUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		return new String(Base64.getDecoder().decode(((String) value).getBytes()));
	}
}
