package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class trims given string
 */
public class TrimstrUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String value = (String) input.get(0);
		return value.trim();
	}

}
