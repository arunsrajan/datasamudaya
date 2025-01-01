package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class evaluates PI
 */
public class PiiUDF extends EvalFunc<Object> {
	@Override
	public Object exec(Tuple input) throws IOException {		
		return Math.PI;
	}

}
