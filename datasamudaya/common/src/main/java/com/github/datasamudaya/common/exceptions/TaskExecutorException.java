package com.github.datasamudaya.common.exceptions;

/**
 * The exception is thrown when a task executor is launched or destroyed.
 * @author Arun
 *
 */
public class TaskExecutorException extends Exception {
	public static final String TASKEXECUTORDESTROYEXCEPTION_MESSAGE = "Exception thrown when task executor is destroyed";
	private static final long serialVersionUID = -7364613010824053199L;
	
	public TaskExecutorException(String message) {
		super(message);
	}
	public TaskExecutorException(String message, Exception parent) {
		super(message, parent);
	}
}
