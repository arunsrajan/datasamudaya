package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class extracts substring of right most chars for a given length
 */
@Getter
@AllArgsConstructor
public class RightcharsUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String value1 = (String) input.get(0);
		Integer lengthtoextract = Integer.valueOf(String.valueOf(input.get(1)));
		return value1.substring(value1.length() - Math.min(lengthtoextract, value1.length()));
	}

}
