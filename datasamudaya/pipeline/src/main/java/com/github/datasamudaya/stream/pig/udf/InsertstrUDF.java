package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class inserts string into given text from starting position to length of string
 */
@Getter
@AllArgsConstructor
public class InsertstrUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String value1 = (String) input.get(0);
		Integer postoinsert = Integer
				.valueOf(String.valueOf(input.get(2)));
		Integer lengthtoinsert = Integer
				.valueOf(String.valueOf(input.get(3)));
		String value2 = (String) input.get(1);
		value2 = value2.substring(0, Math.min(lengthtoinsert, value2.length()));
		return value1.substring(0, Math.min(value1.length(), postoinsert)) + value2
				+ value1.substring(Math.min(value1.length(), postoinsert), value1.length());
	}

}
