package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class locates a string from text
 */
public class LocateUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		String locatetext = (String) input.get(1);
		Integer pos = (Integer) input.get(2);		
		int positiontosearch = Math.min(pos, locatetext.length());
		return positiontosearch + locatetext.substring(positiontosearch).indexOf(text);
	}

}
