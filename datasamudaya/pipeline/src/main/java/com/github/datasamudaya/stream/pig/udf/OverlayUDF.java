package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * The UDF class Replaces text with overlay text
 */
public class OverlayUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		String overlaytext = (String) input.get(1);
		Integer pos = (Integer) input.get(2);
		Integer length = (Integer) input.get(3);
		return text
				.replaceAll(
						text.substring(
								pos, Math
										.min(((String) overlaytext).length(),
												pos	+ length)),
						overlaytext);
	}

}
