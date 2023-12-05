package com.github.datasamudaya.common.execptions;

/**
 * Exception thrown when properties are initialized
 * @author Arun
 *
 */
public class PropertiesException extends Exception {

	public static final String LOADING_PROPERTIES = "Problem in loading properties, See the cause below";
	
	private static final long serialVersionUID = -3562590260349299083L;
	public PropertiesException(String message, Exception parent) {
		super(message, parent);
	}
	public PropertiesException(String message) {
		super(message);
	}
}
