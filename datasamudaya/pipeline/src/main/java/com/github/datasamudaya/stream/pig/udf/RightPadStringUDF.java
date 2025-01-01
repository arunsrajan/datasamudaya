package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class is right pad a String with a specified String.
 */
public class RightPadStringUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		String padtext = (String) input.get(1);
		Integer numpad = (Integer) input.get(2);
		return StringUtils.rightPad(text, numpad, padtext);
	}

}
