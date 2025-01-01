package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class finds index of a text from given text with start position
 */
@Getter
@AllArgsConstructor
public class PositionUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		String indextext = (String) input.get(1);	
		int position = (Integer)  input.get(2);
		return text.indexOf(
				indextext, position);
	}

}
