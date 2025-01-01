package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

/**
 * The UDF class gets the current date and time in date format
 */
public class NowUDF extends EvalFunc<Object> {
	static SimpleDateFormat dateExtract = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Override
	public Object exec(Tuple input) throws IOException {
		return dateExtract.format(new Date(System.currentTimeMillis()));
	}

}
