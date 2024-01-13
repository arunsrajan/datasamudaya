package com.github.datasamudaya.common;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * This class stores YARN resources globally
 * @author arun
 *
 */
public class GlobalYARNResources {
	
	private GlobalYARNResources() {}

	private static final Map<String, Map<String, Object>> yarnresources = new ConcurrentHashMap<>();
	

	/**
	 * get YARN resources by task executor id
	 * @param teid
	 * @return YARN resources
	 */
	public static Map<String, Object> getYarnResourcesByTeId(String teid) {
		return yarnresources.get(teid);
	}
	
	/**
	 * Sets the YARN resources by task executor id
	 * @param teid
	 * @param yarnresources
	 */
	public static void setYarnResourcesByTeId(String teid, Map<String,Object> yarnresources) {
		GlobalYARNResources.yarnresources.put(teid, yarnresources);
	}
	
}
