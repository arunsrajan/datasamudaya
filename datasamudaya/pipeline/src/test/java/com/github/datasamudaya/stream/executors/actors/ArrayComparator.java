package com.github.datasamudaya.stream.executors.actors;

import java.util.Comparator;

import com.github.datasamudaya.common.utils.Utils;

/**
 * Comparator Utils
 */
public class ArrayComparator implements Comparator<Object> {

	@Override
	public int compare(Object o1, Object o2) {
		Object[] comp1 = (Object[]) o1;
		Object[] comp2 = (Object[]) o2;
		for (int indexarr = 0;indexarr < comp1.length;indexarr++) {
			Object value1 = comp1[0].getClass() == Object[].class ? ((Object[]) comp1[0])[indexarr]
					: comp1[indexarr];
			Object value2 = comp2[0].getClass() == Object[].class ? ((Object[]) comp2[0])[indexarr]
					: comp2[indexarr];
			int result = Utils.compareTo(value1, value2);
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}
	


}
