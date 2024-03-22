package com.github.datasamudaya.common.utils;

import java.io.Serializable;
import java.util.List;

import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.NodeIndexKey;

/**
 * The BTree Node 
 * @author Arun
 *
 */
public class BTreeNode implements Serializable{
	NodeIndexKey[] keys;
    int t;
    BTreeNode[] C;
    int n;
    boolean leaf;
    public BTreeNode(int t, boolean leaf) {
        this.keys = new NodeIndexKey[2 * t - 1];
        this.t = t;
        this.C = new BTreeNode[2 * t];
        this.n = 0;
        this.leaf = leaf;       
    }

	public int compare(NodeIndexKey obj1, NodeIndexKey obj2, List<FieldCollationDirection> rfcs) {
		int numofelem = rfcs.size();
		for (int index = 0; index < numofelem; index++) {
			FieldCollationDirection fc = rfcs.get(index);
			String sortOrder = fc.getDirection().name();
			int result = compareTo(obj1, obj2, index);
			if ("DESCENDING".equals(sortOrder)) {
				result = -result;
			}
			if (result != 0) {
				return result;
			}
		}
		return 0;
	}
    
    public static int compareTo(NodeIndexKey obj1, NodeIndexKey obj2, int index) {
		if (obj1.getKey()[index] instanceof Double val1 && obj2.getKey()[index] instanceof Double val2) {
			return val1.compareTo(val2);
		} else if (obj1.getKey()[index] instanceof Long val1 && obj2.getKey()[index] instanceof Long val2) {
			return val1.compareTo(val2);
		} else if (obj1.getKey()[index] instanceof Integer val1 && obj2.getKey()[index] instanceof Integer val2) {
			return val1.compareTo(val2);
		} else if (obj1.getKey()[index] instanceof Float val1 && obj2.getKey()[index] instanceof Float val2) {
			return val1.compareTo(val2);
		} else if (obj1.getKey()[index] instanceof String val1 && obj2.getKey()[index] instanceof String val2) {
			return val1.compareTo(val2);
		}
		return 0;
	}
    
    void insertNonFull(NodeIndexKey k, List<FieldCollationDirection> rfcs) {
        int index = n - 1;
        if (leaf) {
            while (index >= 0 && compare(keys[index], k, rfcs)>0) {
                keys[index + 1] = keys[index];
                index--;
            }
            keys[index + 1] = k;
            n++;
        } else {
            while (index >= 0 && compare(keys[index], k, rfcs)>0) {
            	index--;
            }
            if (C[index + 1].n == 2 * t - 1) {
                splitChild(index + 1, C[index + 1]);
                if (compare(keys[index + 1], k, rfcs)<0) {
                	index++;
                }
            }
            C[index + 1].insertNonFull(k, rfcs);
        }
    }

    void splitChild(int i, BTreeNode y) {
        BTreeNode z = new BTreeNode(y.t, y.leaf);
        z.n = t - 1;
        for (int j = 0; j < t - 1; j++) {
            z.keys[j] = y.keys[j + t];
        }
        if (!y.leaf) {
            for (int j = 0; j < t; j++) {
                z.C[j] = y.C[j + t];
            }
        }
        y.n = t - 1;
        for (int j = n; j > i; j--) {
            C[j + 1] = C[j];
        }
        C[i + 1] = z;
        for (int j = n - 1; j >= i; j--) {
            keys[j + 1] = keys[j];
        }
        keys[i] = y.keys[t - 1];
        n++;
    }

    public void traverse(List<NodeIndexKey> niks) {
        for (int i = 0; i < n; i++) {
            if (!leaf) {
                C[i].traverse(niks);
            }
            niks.add(keys[i]);
        }
        if (!leaf) {
            C[n].traverse(niks);
        }
    }

    BTreeNode search(NodeIndexKey k, List<FieldCollationDirection> rfcs) {
        int index = 0;
        while (index < n && compare( k, keys[index], rfcs)>0) {
        	index++;
        }
        if (index < n && compare( k, keys[index], rfcs) == 0) {
            return this;
        }
        if (leaf) {
            return null;
        }
        return C[index].search(k, rfcs);
    }
}