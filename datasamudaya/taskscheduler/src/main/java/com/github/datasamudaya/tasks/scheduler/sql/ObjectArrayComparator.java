package com.github.datasamudaya.tasks.scheduler.sql;

import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

import org.apache.calcite.rel.RelFieldCollation;

/**
 * Comparator for Objects
 * @author arun
 *
 */
public class ObjectArrayComparator implements Comparator<Object[]>,Serializable {

	private static final long serialVersionUID = -934060963733653946L;
	List<RelFieldCollation> rfcs;

	public ObjectArrayComparator(List<RelFieldCollation> rfcs) {
		this.rfcs = rfcs;
	}

	@Override
	public int compare(Object[] obj1, Object[] obj2) {

		for (int i = 0;i < rfcs.size();i++) {
			RelFieldCollation fc = rfcs.get(i);
			String sortOrder = fc.getDirection().name();
			Object value1 = obj1[0].getClass() == Object[].class ? ((Object[]) obj1[0])[fc.getFieldIndex()]
					: obj1[fc.getFieldIndex()];
			Object value2 = obj2[0].getClass() == Object[].class ? ((Object[]) obj2[0])[fc.getFieldIndex()]
					: obj2[fc.getFieldIndex()];
			int result = compareTo(value1, value2);
			if ("DESCENDING".equals(sortOrder)) {
				result = -result;
			}
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}

	public static int compareTo(Object obj1, Object obj2) {
		if (obj1 instanceof Double val1 && obj2 instanceof Double val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Long val1 && obj2 instanceof Long val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Integer val1 && obj2 instanceof Integer val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof Float val1 && obj2 instanceof Float val2) {
			return val1.compareTo(val2);
		} else if (obj1 instanceof String val1 && obj2 instanceof String val2) {
			return val1.compareTo(val2);
		}
		return 0;
	}

	public List<RelFieldCollation> getRfcs() {
		return rfcs;
	}

}
