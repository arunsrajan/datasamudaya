package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class extracts substring of right most chars for a given length
 */
public class LeftcharsUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String value1 = (String) input.get(0);
		Integer lengthtoextract = Integer
				.valueOf(String.valueOf(input.get(1)));
		return value1.substring(0, Math.min(lengthtoextract, value1.length()));
	}

}
