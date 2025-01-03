package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.GregorianCalendar;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;

import lombok.Getter;

/**
 * The UDF class extracts month from a given date
 */
@Getter
public class MonthUDF extends EvalFunc<Object> implements EvalFuncName {
	String name;
	public MonthUDF(String name) {		
		this.name = name;
	}
	static SimpleDateFormat dateExtract = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return DataSamudayaConstants.EMPTY;
		}
		String text = (String) input.get(0);
		Calendar calendar = new GregorianCalendar();
		try {
			calendar.setTime(dateExtract.parse(text));
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}
		return calendar.get(Calendar.MONTH)+1;
	}

}
