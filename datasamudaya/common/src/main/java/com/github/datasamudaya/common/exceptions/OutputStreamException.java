package com.github.datasamudaya.common.exceptions;

/**
 * Exception thrown when output stream writes the data
 * @author Arun
 *
 */
public class OutputStreamException extends Exception {

	public static final String ERRORCAUGHT_MESSAGE = "Error caught when data processed thru output stream";
	
	private static final long serialVersionUID = 5906202449013209263L;

	public OutputStreamException(String message) {
		super(message);
	}
		
	public OutputStreamException(String message, Exception parent) {
		super(message, parent);
	}
	
}
