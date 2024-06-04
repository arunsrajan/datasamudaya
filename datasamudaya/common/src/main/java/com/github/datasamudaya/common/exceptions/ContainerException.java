package com.github.datasamudaya.common.exceptions;

/**
 * The exception is thrown when a zookeeper object is created or data processed in zookeeper.
 * @author Arun
 *
 */
public class ContainerException extends Exception {
	public static final String CONTAINEREXCEPTION_MESSAGE = "Exception thrown when container is launched";
	private static final long serialVersionUID = -7364613010824053199L;

	public ContainerException(String message) {
		super(message);
	}

	public ContainerException(String message, Exception parent) {
		super(message, parent);
	}
}
