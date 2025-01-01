package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class checks if a String str contains Unicode digits,if yes then concatenate all the digits in str and return it as a String. 
 */
public class GetDigitsUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		return StringUtils.getDigits(text);
	}

}
