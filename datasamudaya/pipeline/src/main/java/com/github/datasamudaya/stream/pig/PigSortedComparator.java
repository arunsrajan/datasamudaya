package com.github.datasamudaya.stream.pig;

import java.io.Serializable;
import java.util.List;

import com.github.datasamudaya.common.functions.SortedComparator;
import com.github.datasamudaya.stream.utils.SQLUtils;

/**
 * Sorter class for pig logical plan
 * @author arun
 *
 */
public class PigSortedComparator implements SortedComparator<Object[]>,Serializable {
	private static final long serialVersionUID = 1792691650779378129L;
	List<SortOrderColumns> columnssortorder;

	public PigSortedComparator(List<SortOrderColumns> sortordercolumns) {
		this.columnssortorder = sortordercolumns;
	}

	@Override
	public int compare(Object[] obj1, Object[] obj2) {

		for (int i = 0;i < columnssortorder.size();i++) {
			Integer index = columnssortorder.get(i).getColumn();
			Boolean isAsc = columnssortorder.get(i).isIsasc();
			Object value1 = ((Object[]) obj1[0])[index];
			Object value2 = ((Object[]) obj2[0])[index];
			int result = SQLUtils.compareTo(value1, value2);
			if (!isAsc) {
				result = -result;
			}
			if (result != 0) {
				return result;
			}
		}
		return 0;

	}

	public List<SortOrderColumns> getCso() {
		return columnssortorder;
	}
}
