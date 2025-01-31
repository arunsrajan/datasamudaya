package com.github.datasamudaya.common.utils;

import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.functions.SortedComparator;

/**
 * Generic NodeIndexKey Comparator
 */
public class NodeIndexKeyComparator implements SortedComparator<NodeIndexKey> {

	private static final long serialVersionUID = -552963529125924967L;

	@Override
	public int compare(NodeIndexKey o1, NodeIndexKey o2) {
		return o1.compareTo(o2);
	}

}
