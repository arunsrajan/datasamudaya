package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class rotate (circular shift) a String of shift characters.
 */
@Getter
@AllArgsConstructor
public class RotateUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		Integer numrorate = (Integer) input.get(1);
		return StringUtils.rotate(text, numrorate);
	}

}
