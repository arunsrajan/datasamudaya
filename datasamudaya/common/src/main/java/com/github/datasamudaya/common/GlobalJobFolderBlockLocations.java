/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.common;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.log4j.Logger;

/**
 * Holds BlocksLocation object globally.
 * @author arun
 *
 */
public class GlobalJobFolderBlockLocations {

	private static Logger log = Logger.getLogger(GlobalJobFolderBlockLocations.class);

	private GlobalJobFolderBlockLocations() {
	}

	private static Boolean isResetBlocksLocation; 
	
	public static Boolean getIsResetBlocksLocation() {
		return isResetBlocksLocation;
	}

	public static void setIsResetBlocksLocation(Boolean isResetBlocksLocation) {
		GlobalJobFolderBlockLocations.isResetBlocksLocation = isResetBlocksLocation;
	}

	private static Map<String, Map<String, List<BlocksLocation>>> lcsmap = new ConcurrentHashMap<>();

	/**
	 * The put method for holding userid as key and list of BlocksLocation object as values.
	 * @param hdfsfolder
	 * @param lbls
	 */
	public static void put(String jobid,String hdfsfolder, List<BlocksLocation> lbls) {
		Map<String, List<BlocksLocation>> folderblockslocationmap = lcsmap.get(jobid);
		if (isNull(folderblockslocationmap)) {
			folderblockslocationmap = new ConcurrentHashMap<>();
			lcsmap.put(jobid, folderblockslocationmap);
		}
		else {
			log.info("Chamber launched already: " + hdfsfolder + " with assets: " + lbls);
		}
		folderblockslocationmap.put(hdfsfolder, lbls);
	}

	/**
	 * Thie method returns list of LaunchContainers objects for a given userid.  
	 * @param hdfsfolderpath
	 * @return list of LaunchContainers object.
	 */
	public static List<BlocksLocation> get(String jobid, String hdfsfolder) {
		return nonNull(lcsmap.get(jobid))?lcsmap.get(jobid).get(hdfsfolder):null;
	}

	/**
	 * Removes the entry of key and its values for the given hdfsfolderpath.
	 * @param hdfsfolderpath
	 */
	public static void remove(String jobid) {
		lcsmap.remove(jobid);
	}
}
