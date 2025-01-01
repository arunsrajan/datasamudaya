package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class repeat a String repeat times to form a new String. 
 */
@Getter
@AllArgsConstructor
public class RepeatSeparatorUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		String repeatseparatortext = (String) input.get(1);
		Integer number = (Integer) input.get(2);
		return StringUtils.repeat(text, repeatseparatortext, number);
	}

}
