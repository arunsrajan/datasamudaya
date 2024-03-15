package com.github.datasamudaya.common.exceptions;

/**
 * The error caught when job is created
 * @author Arun
 *
 */
public class JobException extends Exception {
	public static final String JOBCREATIONEXCEPTION_MESSAGE = "Error caught when job is created";
	private static final long serialVersionUID = -7507881074205009257L;

	public JobException(String message) {
		super(message);
	}

	public JobException(String message, Exception parent) {
		super(message, parent);
	}
}
