package com.github.datasamudaya.common.utils;

import java.io.Serializable;
import java.util.List;

import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.NodeIndexKey;

/**
 * BTree traversal, Insert function and search function
 * @author arun
 *
 */
public class BTree implements Serializable{
	private static final long serialVersionUID = 1435554803986109246L;
	BTreeNode root;
    int t;

    public BTree(int t) {
        this.root = null;
        this.t = t;
    }

    public void traverse(List<NodeIndexKey> niks) {
        if (root != null) {
            root.traverse(niks);
        }
    }

    public BTreeNode search(NodeIndexKey k, List<FieldCollationDirection> rfcs) {
        return root == null ? null : root.search(k, rfcs);
    }

    public void insert(NodeIndexKey k, List<FieldCollationDirection> rfcs) {
        if (root == null) {
            root = new BTreeNode(t, true);
            root.keys[0] = k;
            root.n = 1;
        } else {
            if (root.n == 2 * t - 1) {
                BTreeNode s = new BTreeNode(t, false);
                s.C[0] = root;
                s.splitChild(0, root);
                int i = 0;
                if (s.compare(s.keys[0] , k, rfcs)<0) {
                    i++;
                }
                s.C[i].insertNonFull(k, rfcs);
                root = s;
            } else {
                root.insertNonFull(k, rfcs);
            }
        }
    }
}

