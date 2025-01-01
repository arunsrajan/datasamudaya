package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class gets random number in double format 
 */
public class RandUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {		
		return new Random(System.currentTimeMillis()).nextDouble();
	}

}
