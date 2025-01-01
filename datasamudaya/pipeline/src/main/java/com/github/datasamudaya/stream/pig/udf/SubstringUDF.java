package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class gets substring 
 */
public class SubstringUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		Integer pos = (Integer) input.get(1);
		Integer length = (Integer) input.get(2);
		return text.substring(pos,
				Math.min(text.length(), pos
						+ length));
	}

}
