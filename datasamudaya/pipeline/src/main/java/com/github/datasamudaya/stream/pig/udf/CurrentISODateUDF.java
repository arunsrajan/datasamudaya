package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class evaluates current ISO date 
 */
public class CurrentISODateUDF extends EvalFunc<Object> {
	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	@Override
	public Object exec(Tuple input) throws IOException {		
		return dateFormat.format(new Date(System.currentTimeMillis()));
	}

}
