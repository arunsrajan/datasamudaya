/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.nio.charset.Charset;
import java.util.ArrayList;

import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author arun The helper or utility class for launching container processes.
 */
public class ContainerLauncher {

	private ContainerLauncher() {
	}

	static Logger log = LoggerFactory.getLogger(ContainerLauncher.class);

	/**
	 * Launch the task executor with given parameters
	 * @param port
 * @param diskcache
 * @param cls
 * @param prop
 * @param cr
 * @param jobid
 * @return process object
	 */
	public static Process spawnDataSamudayaContainer(String port, String diskcache, Class<?> cls, String prop,
			ContainerResources cr, String jobid, int numberofexecutors) {

		try {
			var host = (String) DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST);
			var argumentsForSpawn = new ArrayList<String>();
			// Java home from task scheduler with java executable path 
			argumentsForSpawn.add(System.getProperty("java.home").replace("\\", "/") + "/bin/java");
			//Java Class path
			argumentsForSpawn.add("-cp");
			String uniquefilename = String.format(DataSamudayaConstants.CLASSPATHFILE, System.currentTimeMillis());
			Utils.gatherClasspathDependenciesToFile(System.getProperty(DataSamudayaConstants.TMPDIR), uniquefilename);
			argumentsForSpawn.add("\"" + DataSamudayaConstants.AT + System.getProperty(DataSamudayaConstants.TMPDIR)+
					uniquefilename+ "\"");
			//Minimum memory 256MB
			argumentsForSpawn.add("-Xms" + cr.getMinmemory());
			//Maximum heap to launch container.
			argumentsForSpawn.add("-Xmx" + cr.getMaxmemory());
			//Set the Stack size to 128 mb
			argumentsForSpawn.add("-Xss128m");
			//Maximum number of cpu power to restrict execution thread
			argumentsForSpawn.add("-XX:ActiveProcessorCount=" + cr.getCpu());
			argumentsForSpawn
					.add("-Djdk.virtualThreadScheduler.parallelism="+ cr.getCpu());
			argumentsForSpawn
			.add("-Djdk.virtualThreadScheduler.maxPoolSize=256");
			argumentsForSpawn
			.add("-Djdk.virtualThreadScheduler.minRunnable=256");
			//Heap dump on out of memory error configuration
			argumentsForSpawn.add("-XX:+HeapDumpOnOutOfMemoryError");
			//Enable preview features
			argumentsForSpawn.add("--enable-preview");
			//Prefer only IPV4
			argumentsForSpawn.add("-Djava.net.preferIPv4Stack=true");
			//GC XGC or G1GC
			argumentsForSpawn.add(cr.getGctype());
			argumentsForSpawn.add("-D" + DataSamudayaConstants.TASKEXECUTOR_HOST + "=" + host);
			argumentsForSpawn.add("-D" + DataSamudayaConstants.TASKEXECUTOR_PORT + "=" + port);
			argumentsForSpawn.add("-D" + DataSamudayaConstants.CACHEDISKPATH + "=" + diskcache);
			//Task executor class name to launch
			argumentsForSpawn.add(cls.getName());
			argumentsForSpawn.add(prop);
			//Direct heap for unsafe memory.
			argumentsForSpawn.add("" + cr.getDirectheap());
			argumentsForSpawn.add(jobid);
			argumentsForSpawn.add(DataSamudayaConstants.EMPTY + cr.getExecutortype());
			argumentsForSpawn.add(DataSamudayaConstants.EMPTY + numberofexecutors);
			log.debug("Launching Container Daemon Process: " + argumentsForSpawn);
			//Spawning the process for running task executor
			Process process = Runtime.getRuntime()
					.exec(argumentsForSpawn.toArray(new String[argumentsForSpawn.size()]));
			if(!process.isAlive()) {
				log.debug("Process {} started", IOUtils.toString(process.getErrorStream(), Charset.defaultCharset()));	
			}
			
			return process;


		} catch (Exception ex) {
			log.error("Unable able to spawn container: " + cr.getMinmemory() + " " + cr.getMaxmemory()
					+ " " + port + " " + cr.getCpu() + " " + cr.getGctype() + " " + cls.getName(), ex);
		}
		return null;
	}

	/**
	* This static method spawns the Ignite process with given input and returns spawned process object.
	* @param port
	* @param cls
	* @param prop
	* @param cr
	* @return process object
	*/
	public static Process spawnDataSamudayaContainerIgnite(String port, Class<?> cls, String prop,
			ContainerResources cr) {
		try {
			var argumentsForSpawn = new ArrayList<String>();
			argumentsForSpawn.add(System.getProperty("java.home").replace("\\", "/") + "/bin/java");
			argumentsForSpawn.add("-classpath");
			argumentsForSpawn.add(System.getProperty("java.class.path"));
			argumentsForSpawn.add("-Xms" + cr.getMinmemory() + "m");
			argumentsForSpawn.add("-Xmx" + cr.getMaxmemory() + "m");
			argumentsForSpawn.add("-XX:ActiveProcessorCount=" + cr.getCpu());
			argumentsForSpawn.add("-XX:InitiatingHeapOccupancyPercent=40");
			argumentsForSpawn.add("-Xrunjdwp:server=y,transport=dt_socket,address="
					+ (Integer.parseInt(port) + 100) + ",suspend=n");
			argumentsForSpawn.add(cr.getGctype());
			argumentsForSpawn.add(cls.getName());
			argumentsForSpawn.add(prop);
			argumentsForSpawn.add(port);
			log.debug("Launching Ignite Container Daemon Process: " + argumentsForSpawn);
			return Runtime.getRuntime()
					.exec(argumentsForSpawn.toArray(new String[argumentsForSpawn.size()]));

		} catch (Exception ex) {
			log.error("Unable able to spawn container: " + cr.getMinmemory() + " " + cr.getMaxmemory()
					+ " " + port + " " + cr.getCpu() + " " + cr.getGctype() + " " + cls.getName(), ex);
		}
		return null;
	}
}
