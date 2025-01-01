package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class trims only left most of a given text
 */
public class LtrimUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		return StringUtils.stripStart(text, null);
	}

}
