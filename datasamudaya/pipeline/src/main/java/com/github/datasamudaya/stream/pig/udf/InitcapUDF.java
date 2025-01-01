package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class capitalizes the first character in a given text
 */
public class InitcapUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		return StringUtils.capitalize(text);
	}

}
