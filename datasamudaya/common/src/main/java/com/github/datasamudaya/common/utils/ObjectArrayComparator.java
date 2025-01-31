package com.github.datasamudaya.common.utils;

import com.github.datasamudaya.common.functions.SortedComparator;

/**
 * Generic Object Array Comparator
 */
public class ObjectArrayComparator implements SortedComparator<Object[]> {

	private static final long serialVersionUID = -3837817081136845385L;

	/**
	 * Compare Method
	 */
	@Override
	public int compare(Object[] o1, Object[] o2) {
		for (int indexarr = 0;indexarr < o1.length;indexarr++) {
			Object value1 = o1[0].getClass() == Object[].class ? ((Object[]) o1[0])[indexarr]
					: o1[indexarr];
			Object value2 = o2[0].getClass() == Object[].class ? ((Object[]) o2[0])[indexarr]
					: o2[indexarr];
			int result = Utils.compareTo(value1, value2);
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}

}
