package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.util.Base64;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class decodes base64 encoded string
 */
@Getter
@AllArgsConstructor
public class Base64DecodeUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		return new String(Base64.getDecoder().decode(((String) value).getBytes()));
	}
}
