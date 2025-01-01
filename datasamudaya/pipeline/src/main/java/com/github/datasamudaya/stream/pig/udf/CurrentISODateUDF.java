package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import lombok.Getter;

/**
 * The UDF class evaluates current ISO date 
 */
@Getter
public class CurrentISODateUDF extends EvalFunc<Object> implements EvalFuncName {
	String name;
	public CurrentISODateUDF(String name) {
		this.name = name;
	}
	static SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ssZ");
	@Override
	public Object exec(Tuple input) throws IOException {		
		return dateFormat.format(new Date(System.currentTimeMillis()));
	}

}
