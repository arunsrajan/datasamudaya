package com.github.datasamudaya.stream.pig;

public class PigException extends Exception {
	private static final long serialVersionUID = 661011676693914418L;
	public static final String NOALIASFOUND="No Such Alias %s Found";
	public PigException(String message) {
		super(message);
	}
	
}
