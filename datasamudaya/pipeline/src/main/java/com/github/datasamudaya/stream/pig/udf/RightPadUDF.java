package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class is right pad a String with spaces (' ').
 */
@Getter
@AllArgsConstructor
public class RightPadUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		Integer numpad = (Integer) input.get(1);
		return StringUtils.rightPad(text, numpad);
	}

}
