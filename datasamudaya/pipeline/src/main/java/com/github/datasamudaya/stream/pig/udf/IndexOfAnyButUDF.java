package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class extracts  first index of any character not in the given set of characters. 
 */
@Getter
@AllArgsConstructor
public class IndexOfAnyButUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text1 = (String) input.get(0);
		String text2 = (String) input.get(1);
		return StringUtils.indexOfAnyBut(text1, text2);
	}

}
