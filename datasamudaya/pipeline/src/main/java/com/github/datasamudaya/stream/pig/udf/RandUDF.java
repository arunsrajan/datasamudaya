package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.util.Random;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.AllArgsConstructor;
import lombok.Getter;

/**
 * The UDF class gets random number in double format 
 */
@Getter
@AllArgsConstructor 
public class RandUDF extends EvalFunc<Object> implements EvalFuncName {

	String name;
	
	@Override
	public Object exec(Tuple input) throws IOException {		
		return new Random(System.currentTimeMillis()).nextDouble();
	}

}
