package com.github.datasamudaya.common.utils;

import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple1;
import org.jooq.lambda.tuple.Tuple10;
import org.jooq.lambda.tuple.Tuple11;
import org.jooq.lambda.tuple.Tuple12;
import org.jooq.lambda.tuple.Tuple13;
import org.jooq.lambda.tuple.Tuple14;
import org.jooq.lambda.tuple.Tuple15;
import org.jooq.lambda.tuple.Tuple16;
import org.jooq.lambda.tuple.Tuple2;
import org.jooq.lambda.tuple.Tuple3;
import org.jooq.lambda.tuple.Tuple4;
import org.jooq.lambda.tuple.Tuple5;
import org.jooq.lambda.tuple.Tuple6;
import org.jooq.lambda.tuple.Tuple7;
import org.jooq.lambda.tuple.Tuple8;
import org.jooq.lambda.tuple.Tuple9;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;

/**
 * The class serializes tuples of size up to 16.
 * @author Arun
 *
 */
public class TupleSerializer extends Serializer {
	@Override
	public void write(Kryo kryo, Output output, Object tuple) {
		if (tuple instanceof Tuple1 tup) {
			kryo.writeClassAndObject(output, tup.v1());
		} else if (tuple instanceof Tuple2 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
		} else if (tuple instanceof Tuple3 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
		} else if (tuple instanceof Tuple4 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
		} else if (tuple instanceof Tuple5 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
		} else if (tuple instanceof Tuple6 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
		} else if (tuple instanceof Tuple7 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
		} else if (tuple instanceof Tuple8 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
		} else if (tuple instanceof Tuple9 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
		} else if (tuple instanceof Tuple10 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
		} else if (tuple instanceof Tuple11 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
		} else if (tuple instanceof Tuple12 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
			kryo.writeClassAndObject(output, tup.v12());
		} else if (tuple instanceof Tuple13 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
			kryo.writeClassAndObject(output, tup.v12());
			kryo.writeClassAndObject(output, tup.v13());
		} else if (tuple instanceof Tuple14 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
			kryo.writeClassAndObject(output, tup.v12());
			kryo.writeClassAndObject(output, tup.v13());
			kryo.writeClassAndObject(output, tup.v14());
		} else if (tuple instanceof Tuple15 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
			kryo.writeClassAndObject(output, tup.v12());
			kryo.writeClassAndObject(output, tup.v13());
			kryo.writeClassAndObject(output, tup.v14());
			kryo.writeClassAndObject(output, tup.v15());
		} else if (tuple instanceof Tuple16 tup) {
			kryo.writeClassAndObject(output, tup.v1());
			kryo.writeClassAndObject(output, tup.v2());
			kryo.writeClassAndObject(output, tup.v3());
			kryo.writeClassAndObject(output, tup.v4());
			kryo.writeClassAndObject(output, tup.v5());
			kryo.writeClassAndObject(output, tup.v6());
			kryo.writeClassAndObject(output, tup.v7());
			kryo.writeClassAndObject(output, tup.v8());
			kryo.writeClassAndObject(output, tup.v9());
			kryo.writeClassAndObject(output, tup.v10());
			kryo.writeClassAndObject(output, tup.v11());
			kryo.writeClassAndObject(output, tup.v12());
			kryo.writeClassAndObject(output, tup.v13());
			kryo.writeClassAndObject(output, tup.v14());
			kryo.writeClassAndObject(output, tup.v15());
			kryo.writeClassAndObject(output, tup.v16());
		}
	}

	@Override
	public Tuple read(Kryo kryo, Input input, Class type) {
		if (type == Tuple1.class) {
			Object v1 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1);
		} else if (type == Tuple2.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2);
		} else if (type == Tuple3.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3);
		} else if (type == Tuple4.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4);
		} else if (type == Tuple5.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5);
		} else if (type == Tuple6.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6);
		} else if (type == Tuple7.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7);
		} else if (type == Tuple8.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8);
		} else if (type == Tuple9.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9);
		} else if (type == Tuple10.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10);
		} else if (type == Tuple11.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11);
		} else if (type == Tuple12.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			Object v12 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12);
		} else if (type == Tuple13.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			Object v12 = kryo.readClassAndObject(input);
			Object v13 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13);
		} else if (type == Tuple14.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			Object v12 = kryo.readClassAndObject(input);
			Object v13 = kryo.readClassAndObject(input);
			Object v14 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14);
		} else if (type == Tuple15.class) {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			Object v12 = kryo.readClassAndObject(input);
			Object v13 = kryo.readClassAndObject(input);
			Object v14 = kryo.readClassAndObject(input);
			Object v15 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15);
		} else {
			Object v1 = kryo.readClassAndObject(input);
			Object v2 = kryo.readClassAndObject(input);
			Object v3 = kryo.readClassAndObject(input);
			Object v4 = kryo.readClassAndObject(input);
			Object v5 = kryo.readClassAndObject(input);
			Object v6 = kryo.readClassAndObject(input);
			Object v7 = kryo.readClassAndObject(input);
			Object v8 = kryo.readClassAndObject(input);
			Object v9 = kryo.readClassAndObject(input);
			Object v10 = kryo.readClassAndObject(input);
			Object v11 = kryo.readClassAndObject(input);
			Object v12 = kryo.readClassAndObject(input);
			Object v13 = kryo.readClassAndObject(input);
			Object v14 = kryo.readClassAndObject(input);
			Object v15 = kryo.readClassAndObject(input);
			Object v16 = kryo.readClassAndObject(input);
			return Tuple.tuple(v1, v2, v3, v4, v5, v6, v7, v8, v9, v10, v11, v12, v13, v14, v15, v16);
		}
	}
}
