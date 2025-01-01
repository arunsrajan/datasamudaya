package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class removes a substring only if it is at the end of a source string,otherwise returns the source string.
 */
@Getter
@AllArgsConstructor
public class RemoveEndUDF extends EvalFunc<Object> implements EvalFuncName {
	
	String name;

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text = (String) input.get(0);
		String removetext = (String) input.get(1);
		return StringUtils.removeEnd(text, removetext);
	}

}
