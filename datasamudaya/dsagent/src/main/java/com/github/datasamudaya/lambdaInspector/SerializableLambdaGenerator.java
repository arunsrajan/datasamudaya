package com.github.datasamudaya.lambdaInspector;

import org.objectweb.asm.ClassVisitor;
import org.objectweb.asm.FieldVisitor;
import org.objectweb.asm.MethodVisitor;
import org.objectweb.asm.Opcodes;

public class SerializableLambdaGenerator extends ClassVisitor {

	public SerializableLambdaGenerator(ClassVisitor classVisitor) {
		super(Opcodes.ASM9, classVisitor);
	}

	@Override
	public void visit(int version, int access, String name, String signature, String superName, String[] interfaces) {
		super.visit(version, access, name, signature, superName, interfaces);
		// Add 'implements Serializable' to the class declaration
		String[] newInterfaces = new String[interfaces.length + 1];
		System.arraycopy(interfaces, 0, newInterfaces, 0, interfaces.length);
		newInterfaces[interfaces.length] = "java/io/Serializable";
		super.visit(version, access, name, signature, superName, newInterfaces);
	}

	@Override
	public void visitEnd() {
		// Add serialVersionUID field
		FieldVisitor fieldVisitor = super.visitField(Opcodes.ACC_PRIVATE + Opcodes.ACC_STATIC + Opcodes.ACC_FINAL,
				"serialVersionUID", "J", null, null);
		fieldVisitor.visitEnd();

		// Implement writeObject method
		MethodVisitor methodVisitor = super.visitMethod(Opcodes.ACC_PRIVATE, "writeObject",
				"(Ljava/io/ObjectOutputStream;)V", null, null);
		methodVisitor.visitCode();
		methodVisitor.visitInsn(Opcodes.RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();

		// Implement readObject method
		methodVisitor = super.visitMethod(Opcodes.ACC_PRIVATE, "readObject",
				"(Ljava/io/ObjectInputStream;)V", null, null);
		methodVisitor.visitCode();
		methodVisitor.visitInsn(Opcodes.RETURN);
		methodVisitor.visitMaxs(0, 0);
		methodVisitor.visitEnd();

		super.visitEnd();
	}
}
