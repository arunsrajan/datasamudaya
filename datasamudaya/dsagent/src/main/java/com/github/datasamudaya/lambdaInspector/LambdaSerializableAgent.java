package com.github.datasamudaya.lambdaInspector;

import java.lang.instrument.ClassFileTransformer;
import java.lang.instrument.IllegalClassFormatException;
import java.lang.instrument.Instrumentation;
import java.security.ProtectionDomain;

import javassist.CannotCompileException;
import javassist.ClassPool;
import javassist.CtClass;
import javassist.CtConstructor;
import javassist.CtMethod;
import javassist.expr.ConstructorCall;

public class LambdaSerializableAgent {
	public static void agentmain(String agentArgs, Instrumentation inst) {
		premain(agentArgs, inst);
	}

	public static void premain(String agentArgs, Instrumentation inst) {
		inst.addTransformer(new InnerClassLambdaMetafactoryTransformer(), true);
		try {
			
			inst.retransformClasses(Class.forName("java.lang.invoke.LambdaMetafactory"));			
		} catch (Exception e) {
			e.printStackTrace();
		}
	}

	private static final class InnerClassLambdaMetafactoryTransformer implements ClassFileTransformer {
		@Override
		public byte[] transform(ClassLoader loader, String className, Class<?> classBeingRedefined,
				ProtectionDomain protectionDomain, byte[] classfileBuffer) throws IllegalClassFormatException {
			try {
				if (className.equals("java/lang/invoke/InnerClassLambdaMetafactory")) {
					ClassPool cp = ClassPool.getDefault();
					try (var bais = new java.io.ByteArrayInputStream(classfileBuffer)) {
						CtClass cc = cp.makeClass(bais);
						CtConstructor ctor = cc.getConstructors()[0];	
						ctor.instrument(new javassist.expr.ExprEditor() {
					        public void edit(ConstructorCall ccall)
				                      throws CannotCompileException
				        {									     
					        	ccall.replace("{$_ = $proceed($1,$2,$3,$4,$5,$6,true,$8,$9); }");
				        }
						});
						cc.detach();
						return cc.toBytecode();
					}
				} else if (className.equals("java/lang/invoke/LambdaMetafactory")) {
					ClassPool cp = ClassPool.getDefault();
					try (var bais = new java.io.ByteArrayInputStream(classfileBuffer)) {
						CtClass cc = cp.makeClass(bais);
						CtMethod[] methods = cc.getMethods();
						for (CtMethod method : methods) {
							if (method.getName().equals("metafactory")) {								
											method.setBody("""
													{
                                                        java.lang.invoke.AbstractValidatingLambdaMetafactory mf;
                                                        mf = new java.lang.invoke.InnerClassLambdaMetafactory((java.lang.invoke.MethodHandles.Lookup)java.util.Objects.requireNonNull($1),
                                                                (java.lang.invoke.MethodType)java.util.Objects.requireNonNull($3),
                                                                (String)java.util.Objects.requireNonNull($2),
                                                                (java.lang.invoke.MethodType)java.util.Objects.requireNonNull($4),
                                                                (java.lang.invoke.MethodHandle)java.util.Objects.requireNonNull($5),
                                                                (java.lang.invoke.MethodType)java.util.Objects.requireNonNull($6),
                                                                true,
                                                                EMPTY_CLASS_ARRAY,
                                                                EMPTY_MT_ARRAY);
                                                        mf.validateMetafactoryArgs();
                                                        return mf.buildCallSite();
                                                    }
                                                    """);
													
							}
						}
						cc.detach();
						return cc.toBytecode();
					}
				}
				
			} catch (Exception e) {
				e.printStackTrace();
			}
			return classfileBuffer;
		}
	}
}
