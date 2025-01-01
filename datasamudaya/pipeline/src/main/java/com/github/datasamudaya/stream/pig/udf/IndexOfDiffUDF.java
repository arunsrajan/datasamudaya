package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class returns the index at which the CharSequences begin to differ.
 */
public class IndexOfDiffUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		String text1 = (String) input.get(0);
		String text2 = (String) input.get(1);
		return StringUtils.indexOfDifference(text1, text2);
	}

}
