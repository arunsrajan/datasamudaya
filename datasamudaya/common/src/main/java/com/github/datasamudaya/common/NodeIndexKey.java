package com.github.datasamudaya.common;

import java.io.Serializable;
import java.util.Arrays;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
@ToString
public class NodeIndexKey implements Serializable {	
	private static final long serialVersionUID = 1L;
	private String node;
	private Integer index;
	private Object[] key;
	private Object value;
	private NodeIndexKey left;
	private NodeIndexKey right;
	private String cachekey;
	private Task task;
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		NodeIndexKey other = (NodeIndexKey) obj;
		Logger log = LoggerFactory.getLogger(NodeIndexKey.class);
		return Arrays.deepEquals((Object[])value, (Object[])other.value);
	}
	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + Arrays.deepHashCode((Object[])value);
		return result;
	}
	
}
