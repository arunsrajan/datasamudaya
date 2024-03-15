package com.github.datasamudaya.common.exceptions;

/**
 * The exception caught when launching YARN container
 * @author Arun
 *
 */
public class YarnLaunchException extends Exception {

	public static final String YARNLAUNCHEXCEPTION = "Error caught when launching YARN container";
	private static final long serialVersionUID = 8068768938761963004L;

	public YarnLaunchException(String message) {
		super(message);
	}

	public YarnLaunchException(String message, Exception parent) {
		super(message, parent);
	}
}
