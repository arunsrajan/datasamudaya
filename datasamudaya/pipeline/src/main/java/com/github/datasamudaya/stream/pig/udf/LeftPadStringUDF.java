package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class pads empty characters at the end of text including the size of given text
 */
@Getter
@AllArgsConstructor
public class LeftPadStringUDF extends EvalFunc<Object> {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		String padtext = (String) input.get(1);
		Integer numpadsize = (Integer) input.get(2); 
		return StringUtils.leftPad(text, numpadsize, padtext);
	}

}
