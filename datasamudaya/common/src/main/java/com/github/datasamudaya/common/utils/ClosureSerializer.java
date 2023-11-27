package com.github.datasamudaya.common.utils;

import static com.esotericsoftware.kryo.util.Util.className;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Method;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

/**
 * The modified closure serializer for the bug detected when creating the 
 * object using Class.forName with class loader.   
 * @author arun
 *
 */
public class ClosureSerializer extends Serializer {

	public static class Closure {
		
	}
	
	private static Method readResolve;

	public ClosureSerializer() {
		if (readResolve == null) {
			try {
				readResolve = SerializedLambda.class.getDeclaredMethod("readResolve");
				readResolve.setAccessible(true);
			} catch (Exception ex) {
				readResolve = null;
				Log.warn("""
					Unable to obtain SerializedLambda#readResolve via reflection. \
					Falling back on resolving lambdas via capturing class.\
					""", ex);
			}
		}
	}
	/**
	 * The write method
	 */
	public void write(Kryo kryo, Output output, Object object) {
		SerializedLambda serializedLambda = toSerializedLambda(object);
		int count = serializedLambda.getCapturedArgCount();
		output.writeVarInt(count, true);
		for (int i = 0;i < count;i++) {
			kryo.writeClassAndObject(output, serializedLambda.getCapturedArg(i));
		}
		try {
			if(nonNull(kryo.getClassLoader())) {
				kryo.writeClass(output, Class.forName(serializedLambda.getCapturingClass().replace('/', '.'), true, kryo.getClassLoader()));
			} else {
				kryo.writeClass(output, Class.forName(serializedLambda.getCapturingClass().replace('/', '.')));
			}
		} catch (ClassNotFoundException ex) {
			throw new KryoException("Error writing closure.", ex);
		}
		output.writeString(serializedLambda.getFunctionalInterfaceClass());
		output.writeString(serializedLambda.getFunctionalInterfaceMethodName());
		output.writeString(serializedLambda.getFunctionalInterfaceMethodSignature());
		output.writeVarInt(serializedLambda.getImplMethodKind(), true);
		output.writeString(serializedLambda.getImplClass());
		output.writeString(serializedLambda.getImplMethodName());
		output.writeString(serializedLambda.getImplMethodSignature());
		output.writeString(serializedLambda.getInstantiatedMethodType());
	}

	/**
	 * The read method.
	 */
	public Object read(Kryo kryo, Input input, Class type) {
		int count = input.readVarInt(true);
		Object[] capturedArgs = new Object[count];
		for (int i = 0;i < count;i++) {
			capturedArgs[i] = kryo.readClassAndObject(input);
		}
		Class<?> capturingClass = kryo.readClass(input).getType();
		SerializedLambda serializedLambda = new SerializedLambda(capturingClass, input.readString(),
			input.readString(), input.readString(), input.readVarInt(true), input.readString(), input.readString(),
			input.readString(), input.readString(), capturedArgs);
		try {
			return readResolve(capturingClass, serializedLambda);
		} catch (Exception ex) {
			throw new KryoException("Error reading closure.", ex);
		}
	}
	/**
	 * The copy method from original to cloned. 
	 * @param kryo
	 * @param original
	 * @return
	 */
	public Closure copy(Kryo kryo, Closure original) {
		try {
			SerializedLambda lambda = toSerializedLambda(original);
			Class<?> capturingClass = null;
			if(nonNull(kryo.getClassLoader())) {
				capturingClass = Class.forName(lambda.getCapturingClass().replace('/', '.'), true, kryo.getClassLoader());
			} else {
				capturingClass = Class.forName(lambda.getCapturingClass().replace('/', '.'));
			}
			return (Closure) readResolve(capturingClass, lambda);
		} catch (Exception ex) {
			throw new KryoException("Error copying closure.", ex);
		}
	}

	private Object readResolve(Class<?> capturingClass, SerializedLambda lambda) throws Exception {
		if (readResolve != null) {
			return readResolve.invoke(lambda);
		}

		// See SerializedLambda#readResolve
		Method m = capturingClass.getDeclaredMethod("$deserializeLambda$", SerializedLambda.class);
		m.setAccessible(true);
		return m.invoke(null, lambda);
	}

	private SerializedLambda toSerializedLambda(Object object) {
		Object replacement;
		try {
			Method writeReplace = object.getClass().getDeclaredMethod("writeReplace");
			writeReplace.setAccessible(true);
			replacement = writeReplace.invoke(object);
		} catch (Exception ex) {
			if (object instanceof Serializable) {
				throw new KryoException("Error serializing closure.", ex);
			}
			throw new KryoException("Closure must implement java.io.Serializable.", ex);
		}
		try {
			return (SerializedLambda) replacement;
		} catch (Exception ex) {
			throw new KryoException("writeReplace must return a SerializedLambda: " + className(replacement.getClass()), ex);
		}
	}

}
