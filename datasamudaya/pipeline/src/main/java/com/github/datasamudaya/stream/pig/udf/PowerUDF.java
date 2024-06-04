package com.github.datasamudaya.stream.pig.udf;

import java.io.IOException;

import org.apache.pig.EvalFunc;
import org.apache.pig.data.Tuple;

public class PowerUDF extends EvalFunc<Object> {

	@Override
	public Object exec(Tuple input) throws IOException {
		if (input == null || input.size() == 0) {
			return 0;
		}
		Object value = (Object) input.get(0);
		Object powerval = (Object) input.get(1);
		if (value instanceof Double pdv && powerval instanceof Integer powval) {
			return Math.pow(pdv, powval);
		} else if (value instanceof Long plv && powerval instanceof Integer powval) {
			return Math.pow(plv, powval);
		} else if (value instanceof Float pfv && powerval instanceof Integer powval) {
			return Math.pow(pfv, powval);
		} else if (value instanceof Integer piv && powerval instanceof Integer powval) {
			return Math.pow(piv, powval);
		} else if (value instanceof Double pdv && powerval instanceof Double powval) {
			return Math.pow(pdv, powval);
		} else if (value instanceof Long plv && powerval instanceof Double powval) {
			return Math.pow(plv, powval);
		} else if (value instanceof Float pfv && powerval instanceof Double powval) {
			return Math.pow(pfv, powval);
		} else if (value instanceof Integer piv && powerval instanceof Double powval) {
			return Math.pow(piv, powval);
		} else if (value instanceof Double pdv && powerval instanceof Float powval) {
			return Math.pow(pdv, powval);
		} else if (value instanceof Long plv && powerval instanceof Float powval) {
			return Math.pow(plv, powval);
		} else if (value instanceof Float pfv && powerval instanceof Float powval) {
			return Math.pow(pfv, powval);
		} else if (value instanceof Integer piv && powerval instanceof Float powval) {
			return Math.pow(piv, powval);
		} else if (value instanceof Double pdv && powerval instanceof Long powval) {
			return Math.pow(pdv, powval);
		} else if (value instanceof Long plv && powerval instanceof Long powval) {
			return Math.pow(plv, powval);
		} else if (value instanceof Float pfv && powerval instanceof Long powval) {
			return Math.pow(pfv, powval);
		} else if (value instanceof Integer piv && powerval instanceof Long powval) {
			return Math.pow(piv, powval);
		}
		return value;
	}
}
