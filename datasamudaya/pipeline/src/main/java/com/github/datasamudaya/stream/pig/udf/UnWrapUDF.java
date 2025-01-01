package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class unwraps a given String from another String.
 */
public class UnWrapUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		String wraptext = (String) input.get(1);
		return StringUtils.unwrap(text, wraptext);
	}

}
