package com.github.datasamudaya.tasks.scheduler.exception;

public class ApplicationSubmitterException extends Exception{
	private static final long serialVersionUID = 988068241024869590L;

	public ApplicationSubmitterException(Exception ex, String message) {
        super(message, ex);
    }
}
