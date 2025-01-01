package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class is case insensitive removal of all occurrences of a substring from withinthe source string.
 */
public class RemoveIgnoreCaseUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		String removetext = (String) input.get(1);
		return StringUtils.removeIgnoreCase(text, removetext);
	}

}
