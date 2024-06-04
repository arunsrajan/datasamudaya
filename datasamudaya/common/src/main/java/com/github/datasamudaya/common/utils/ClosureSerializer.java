package com.github.datasamudaya.common.utils;

import static com.esotericsoftware.kryo.util.Util.className;
import static java.util.Objects.nonNull;

import java.io.Serializable;
import java.lang.invoke.CallSite;
import java.lang.invoke.LambdaMetafactory;
import java.lang.invoke.MethodHandle;
import java.lang.invoke.MethodHandleInfo;
import java.lang.invoke.MethodHandles.Lookup;
import java.lang.invoke.MethodType;
import java.lang.invoke.SerializedLambda;
import java.lang.reflect.Constructor;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.KryoException;
import com.esotericsoftware.kryo.Serializer;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.esotericsoftware.minlog.Log;

public class ClosureSerializer extends Serializer {

	private static final Logger log = LoggerFactory.getLogger(ClosureSerializer.class);

	public static class Closure {
	}

	private static Method readResolve;
	private static Field capturingClass;
	private static Constructor<Lookup> lookupConstructor;

	public ClosureSerializer() {
		if (readResolve == null) {
			try {
				readResolve = SerializedLambda.class.getDeclaredMethod("readResolve");
				readResolve.setAccessible(true);
			} catch (Exception ex) {
				readResolve = null;
				Log.warn("Unable to obtain SerializedLambda#readResolve via reflection. "
						+ "Falling back on resolving lambdas via capturing class.", ex);
			}
		}
		if (capturingClass == null) {
			try {
				capturingClass = SerializedLambda.class.getDeclaredField("capturingClass");
				capturingClass.setAccessible(true);
			} catch (Exception ex) {
				capturingClass = null;
				Log.warn("Unable to obtain SerializedLambda#capturingClass via reflection. "
						+ "Falling back to resolving capturing class via Class.forName.", ex);
			}
		}

		try {
			lookupConstructor = Lookup.class.getDeclaredConstructor(Class.class);
			lookupConstructor.setAccessible(true);
		} catch (NoSuchMethodException | SecurityException e) {
			throw new RuntimeException(e);
		}
	}

	public void write(Kryo kryo, Output output, Object object) {
		SerializedLambda serializedLambda = toSerializedLambda(object);
		int count = serializedLambda.getCapturedArgCount();
		output.writeVarInt(count, true);
		for (int i = 0;i < count;i++) {
			kryo.writeClassAndObject(output, serializedLambda.getCapturedArg(i));
		}
		try {
			kryo.writeClass(output, getCapturingClass(serializedLambda, kryo));
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

	public Object read(Kryo kryo, Input input, Class type) {
		int count = input.readVarInt(true);
		Object[] capturedArgs = new Object[count];
		for (int i = 0;i < count;i++) {
			capturedArgs[i] = kryo.readClassAndObject(input);
		}
		Class<?> capturingClass = kryo.readClass(input).getType();
		SerializedLambda serializedLambda = new SerializedLambda(capturingClass, input.readString(), input.readString(),
				input.readString(), input.readVarInt(true), input.readString(), input.readString(), input.readString(),
				input.readString(), capturedArgs);
		try {
			return readResolve(capturingClass, serializedLambda);
		} catch (Exception ex) {
			throw new KryoException("Error reading closure.", ex);
		}
	}

	public Object copy(Kryo kryo, Object original) {
		try {
			SerializedLambda lambda = toSerializedLambda(original);
			Class<?> capturingClass = getCapturingClass(lambda, kryo);
			return readResolve(capturingClass, lambda);
		} catch (Exception ex) {
			throw new KryoException("Error copying closure.", ex);
		}
	}

	private Object readResolve(Class<?> capturingClass, SerializedLambda lambda) throws Exception {
		ClassLoader cl = capturingClass.getClassLoader();
		Class<?> implClass = cl.loadClass(lambda.getImplClass().replace('/', '.'));
		Class<?> interfaceType = cl.loadClass(lambda.getFunctionalInterfaceClass().replace('/', '.'));
		log.info("Capturing Class {} Impl Class {} InterfaceType {}", capturingClass, implClass, interfaceType);
		Lookup lookup = getLookup(implClass);
		MethodType implType = MethodType.fromMethodDescriptorString(lambda.getImplMethodSignature(), cl);
		MethodType samType = MethodType.fromMethodDescriptorString(lambda.getFunctionalInterfaceMethodSignature(),
				null);

		MethodHandle implMethod;
		boolean implIsInstanceMethod = true;
		switch (lambda.getImplMethodKind()) {
			case MethodHandleInfo.REF_invokeInterface:
			case MethodHandleInfo.REF_invokeVirtual:
				implMethod = lookup.findVirtual(implClass, lambda.getImplMethodName(), implType);
				break;
			case MethodHandleInfo.REF_invokeSpecial:
				implMethod = lookup.findSpecial(implClass, lambda.getImplMethodName(), implType, implClass);
				break;
			case MethodHandleInfo.REF_invokeStatic:
				implMethod = lookup.findStatic(implClass, lambda.getImplMethodName(), implType);
				implIsInstanceMethod = false;
				break;
			default:
				throw new RuntimeException("Unsupported impl method kind " + lambda.getImplMethodKind());
		}

		// determine type of factory
		MethodType factoryType = MethodType.methodType(interfaceType,
				Arrays.copyOf(implType.parameterArray(), implType.parameterCount() - samType.parameterCount()));
		if (implIsInstanceMethod) {
			factoryType = factoryType.insertParameterTypes(0, implClass);
		}

		// determine type of method with implements the SAM
		MethodType instantiatedType = implType;
		if (implType.parameterCount() > samType.parameterCount()) {
			instantiatedType = implType.dropParameterTypes(0, implType.parameterCount() - samType.parameterCount());
		}

		// call factory
		CallSite callSite = LambdaMetafactory.altMetafactory(lookup, lambda.getFunctionalInterfaceMethodName(),
				factoryType, samType, implMethod, instantiatedType, 1);

		// invoke callsite
		Object[] capturedArgs = new Object[lambda.getCapturedArgCount()];
		for (int i = 0;i < lambda.getCapturedArgCount();i++) {
			capturedArgs[i] = lambda.getCapturedArg(i);
		}
		try {
			return callSite.dynamicInvoker().invokeWithArguments(capturedArgs);
		} catch (Throwable e) {
			e.printStackTrace();
		}
		return null;
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
			throw new KryoException("writeReplace must return a SerializedLambda: " + className(replacement.getClass()),
					ex);
		}
	}

	private static Class<?> getCapturingClass(SerializedLambda serializedLambda, Kryo kryo)
			throws ClassNotFoundException {
		if (nonNull(kryo.getClassLoader())) {
			return Class.forName(serializedLambda.getCapturingClass().replace('/', '.'), true, kryo.getClassLoader());
		} else {
			return Class.forName(serializedLambda.getCapturingClass().replace('/', '.'));
		}
	}

	private Lookup getLookup(Class<?> owner)
			throws InstantiationException, IllegalAccessException, InvocationTargetException {
		return lookupConstructor.newInstance(owner);
	}
}
