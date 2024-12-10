package com.github.datasamudaya.common;

import java.io.Serializable;

import lombok.AllArgsConstructor;
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
}
