package com.github.datasamudaya.stream.executors.actors;

import java.util.Random;

public class ObjectArrayTestGenerator {
	public static Object[] generate(int totalvalues) {
		Random random = new Random(System.currentTimeMillis());
		int i=0;
		Object[] objects = new Object[totalvalues];
		while(i<totalvalues) {
			objects[i++] = new Object[]{random.nextInt(1),random.nextInt(2),random.nextInt(3)};	
		}
		return objects;
	}
}
