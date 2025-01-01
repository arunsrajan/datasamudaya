package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class extracts last index from text for given search text with position
 */
@Getter
@AllArgsConstructor
public class LastIndexOfStartPosUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text1 = (String) input.get(0);
		String text2 = (String) input.get(1);
		Integer position = (Integer) input.get(2);
		return StringUtils.lastIndexOf(text1, text2, position);
	}

}
