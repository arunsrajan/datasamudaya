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

import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.utils.Utils;

/**
 * Holds BlocksLocation object globally.
 * @author arun
 *
 */
public class GlobalJobFolderBlockLocations {

	private static final Logger log = Logger.getLogger(GlobalJobFolderBlockLocations.class);

	private GlobalJobFolderBlockLocations() {
	}

	private static final Map<String, Map<String, List<BlocksLocation>>> lcsmap = new ConcurrentHashMap<>();
	private static final Map<String, Map<String, List<Path>>> paths = new ConcurrentHashMap<>();
	private static final Map<String, Map<Path, String>> pathwithmd5has = new ConcurrentHashMap<>();

	/**
	 * The put method for holding userid as key and list of BlocksLocation object as values.
	 * @param hdfsfolder
	 * @param lbls
	 */
	public static void put(String jobid, String hdfsfolder, List<BlocksLocation> lbls) {
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
	
	/**
	 * The function puts path with the given jobid folder and paths
	 * @param jobid
	 * @param hdfsfolder
	 * @param latestpaths
	 */
	public static void putPaths(String jobid, String hdfsfolder, List<Path> latestpaths, FileSystem hdfs) {
		Map<String, List<Path>> folderpathsmap = paths.get(jobid);
		if (isNull(folderpathsmap)) {
			folderpathsmap = new ConcurrentHashMap<>();
			paths.put(jobid, folderpathsmap);
		}
		else {
			log.info("Chamber launched already: " + hdfsfolder + " with paths: " + latestpaths);
		}
		folderpathsmap.put(hdfsfolder, latestpaths);
		Map<Path,String> newchecksums = Utils.getCheckSum(latestpaths, hdfs);
		Map<Path, String> checksumsold = pathwithmd5has.get(jobid);
		if(nonNull(checksumsold)) {
			checksumsold.putAll(newchecksums);
		} else {
			pathwithmd5has.put(jobid, newchecksums);
		}
	}
	
	/**
	 * This function returns current paths for the given jobid and folder
	 * @param jobid
	 * @param hdfsfolder
	 * @return returns the paths
	 */
	public static List<Path> getPaths(String jobid, String hdfsfolder) {
		return nonNull(paths.get(jobid))?paths.get(jobid).get(hdfsfolder):null;
	}
	
	/**
	 * This function returns the set of paths to process again if there is change in file content
	 * @param jobid
	 * @param hdfsfolder
	 * @param currentpaths
	 * @param hdfs
	 * @return path to process
	 */
	public static Set<Path> compareCurrentPathsNewPathsAndStore(String jobid, String hdfsfolder, List<Path> currentpaths, FileSystem hdfs){
		Map<String, List<Path>> folderpathsmap = paths.get(jobid);
		Set<Path> newpathtoprocess = new LinkedHashSet<>();
		if(nonNull(folderpathsmap)) {			
			Map<Path,String> newchecksums = Utils.getCheckSum(currentpaths, hdfs);
			Map<Path,String> oldchecksums = pathwithmd5has.get(jobid);
			if(nonNull(oldchecksums)) {
				for(Path path: currentpaths) {
					if(nonNull(oldchecksums.get(path))) {
						if(!newchecksums.get(path).equals(oldchecksums.get(path))) {
							newpathtoprocess.add(path);
						}
					} else {
						newpathtoprocess.add(path);
					}
				}
			}
			pathwithmd5has.put(jobid, newchecksums);
			folderpathsmap.put(hdfsfolder, currentpaths);
		}
		return newpathtoprocess;
	}
	
	
}
