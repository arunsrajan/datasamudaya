package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class extracts index from text for given search text with position
 */
public class IndexOfStartPosUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text1 = (String) input.get(0);
		String text2 = (String) input.get(1);
		Integer position = (Integer) input.get(2);
		return StringUtils.indexOf(text1, text2, position);
	}

}
