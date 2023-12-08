package com.github.datasamudaya.common.exceptions;

/**
 * The exception caught when data is processed in hdfs
 * @author arun
 *
 */
public class HdfsException extends Exception {

	private static final long serialVersionUID = -789219298347614773L;
	
	public HdfsException(String message) {
		super(message);
	}
	
	public HdfsException(String message, Exception parent) {
		super(message, parent);
	}
}
