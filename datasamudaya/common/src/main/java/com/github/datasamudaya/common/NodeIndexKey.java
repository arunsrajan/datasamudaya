package com.github.datasamudaya.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class NodeIndexKey implements Serializable {

	private static final long serialVersionUID = 1L;
	private String node;
	private Integer index;
	private Object[] key;
	private Object[] value;
	private NodeIndexKey left;
	private NodeIndexKey right;
}
