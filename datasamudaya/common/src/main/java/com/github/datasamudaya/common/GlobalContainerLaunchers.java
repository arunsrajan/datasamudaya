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

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import org.apache.log4j.Logger;

import static java.util.Objects.isNull;

/**
 * Holds LaunchContainer object globally.
 * @author arun
 *
 */
public class GlobalContainerLaunchers {

	private static Logger log = Logger.getLogger(GlobalContainerLaunchers.class);

	private GlobalContainerLaunchers() {
	}

	private static Map<String, Map<String,List<LaunchContainers>>> lcsmap = new ConcurrentHashMap<>();

	/**
	 * The put method for holding userid as key, jobid and list of LaunchContainers object as values.
	 * @param userid
	 * @param jobid
	 * @param lcs
	 */
	public static void put(String userid, String jobid, List<LaunchContainers> lcs) {
		Map<String,List<LaunchContainers>> jobidcontainersmap = lcsmap.get(userid);
		if (isNull(jobidcontainersmap)) {
			jobidcontainersmap = new ConcurrentHashMap<>();
			lcsmap.put(userid, jobidcontainersmap);
		}
		else {
			log.info("Chamber launched already: " + userid + " with assets: " + lcs);
		}
		jobidcontainersmap.put(jobid, lcs);
	}

	/**
	 * Get all the containers of all the users.
	 * @return
	 */
	public static List<LaunchContainers> getAll() {
		return lcsmap.keySet().stream().flatMap(userid -> lcsmap.get(userid).entrySet().stream()).flatMap(es->es.getValue().stream()).collect(Collectors.toList());
	}
	
	/**
	 * Get All Users Job Containers Map for printing in web console
	 * @return map of User Job Containers
	 */
	public static Map<String, Map<String,List<LaunchContainers>>> getUserContainersMap() {
		return lcsmap;
	}

	/**
	 * Thie method returns list of LaunchContainers objects for a given userid.  
	 * @param userid
	 * @return list of LaunchContainers object.
	 */
	public static List<LaunchContainers> get(String userid, String jobid) {
		return lcsmap.get(userid).get(jobid);
	}

	/**
	 * Removes the entry of key and its values for the given userid.
	 * @param userid
	 */
	public static void remove(String userid) {
		lcsmap.remove(userid);
	}
	
	public static void remove(String userid, String jobid) {
		lcsmap.get(userid).remove(jobid);
	}
	
}
