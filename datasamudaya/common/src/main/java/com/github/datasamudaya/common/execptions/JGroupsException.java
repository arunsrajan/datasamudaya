package com.github.datasamudaya.common.execptions;

/**
 * Exception thrown when jgroups initialized or data processed through jgroups library.
 * @author Arun
 *
 */
public class JGroupsException extends Exception {
	private static final long serialVersionUID = 4295396012086336862L;

	public static final String JGROUPS_PROCESSING_EXCETPION = "Unable to process the message through jgroups";
	
	public JGroupsException(String message) {
		super(message);
	}
	
	public JGroupsException(String message,Exception parent) {
		super(message, parent);
	}
	
}
