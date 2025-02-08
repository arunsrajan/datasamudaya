package com.github.datasamudaya.common;

import java.io.Serializable;
import java.util.Objects;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The class used in transmitting object from parent actor to child actor
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class OutputObject implements Command,Serializable {
	private static final long serialVersionUID = -2703111261796356369L;
	Object value;
	boolean left;
	boolean right;
	Class<?> terminiatingclass;
	@Override
	public int hashCode() {
		return Objects.hash(left, right, terminiatingclass);
	}
	@Override
	public boolean equals(Object obj) {
		if (this == obj)
			return true;
		if (obj == null)
			return false;
		if (getClass() != obj.getClass())
			return false;
		OutputObject other = (OutputObject) obj;
		return left == other.left && right == other.right && Objects.equals(terminiatingclass, other.terminiatingclass);
	}
}
