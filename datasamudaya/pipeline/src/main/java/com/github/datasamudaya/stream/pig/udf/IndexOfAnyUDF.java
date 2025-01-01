package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class extracts index from text for given search characters
 */
@Getter
@AllArgsConstructor
public class IndexOfAnyUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text1 = (String) input.get(0);
		String text2 = (String) input.get(1);
		return StringUtils.indexOfAny(text1, text2);
	}

}
