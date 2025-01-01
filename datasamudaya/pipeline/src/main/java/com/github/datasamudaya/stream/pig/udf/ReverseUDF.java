package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.commons.lang3.StringUtils;
import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class returns reverse of a given text
 */
@Getter
@AllArgsConstructor
public class ReverseUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;	
	
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String texttoreverse = (String) input.get(0);
		return StringUtils.reverse(texttoreverse);
	}

}
