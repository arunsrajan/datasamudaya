package com.github.datasamudaya.common.exceptions;

/**
 * The exception is thrown when RPC registry is obtained or method is called when data is processed
 * @author Arun
 *
 */
public class RpcRegistryException extends Exception {
	public static final String REGISTRYCREATE_MESSAGE = "Error thrown when rpc is obtained from registry or data is processed through RPC";
	private static final long serialVersionUID = 62208920490729303L;

	public RpcRegistryException(String message) {
		super(message);
	}
	
	public RpcRegistryException(String message, Exception parent) {
		super(message, parent);
	}
	
}
