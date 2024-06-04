package com.github.datasamudaya.common.exceptions;

/**
 * The exception is thrown when a zookeeper object is created or data processed in zookeeper.
 * @author Arun
 *
 */
public class ZookeeperException extends Exception {
	public static final String ZKEXCEPTION_MESSAGE = "Exception thrown when processing zk";
	private static final long serialVersionUID = -7364613010824053199L;

	public ZookeeperException(String message) {
		super(message);
	}

	public ZookeeperException(String message, Exception parent) {
		super(message, parent);
	}
}
