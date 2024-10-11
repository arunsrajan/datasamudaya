/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor;

import static java.util.Objects.nonNull;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.AllocateContainers;
import com.github.datasamudaya.common.ContainerLauncher;
import com.github.datasamudaya.common.DestroyContainer;
import com.github.datasamudaya.common.DestroyContainers;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.LaunchContainers;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.SkipToNewLine;
import com.github.datasamudaya.common.TaskExecutorShutdown;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

/**
 * The node runner class.
 * @author arun
 *
 */
public class NodeRunner implements Callable<Object> {
	private static final Logger log = LoggerFactory.getLogger(NodeRunner.class);
	String proploaderpath;
	ConcurrentMap<String, Map<String, Process>> containerprocesses;
	FileSystem hdfs;
	ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads;
	ConcurrentMap<String, List<Integer>> containeridports;
	Object receivedobject;
	ZookeeperOperations zo;

	public NodeRunner(String proploaderpath,
			ConcurrentMap<String, Map<String, Process>> containerprocesses, FileSystem hdfs,
			ConcurrentMap<String, Map<String, List<Thread>>> containeridcontainerthreads,
			ConcurrentMap<String, List<Integer>> containeridports, Object receivedobject,
			ZookeeperOperations zo) {
		this.proploaderpath = proploaderpath;
		this.containerprocesses = containerprocesses;
		this.hdfs = hdfs;
		this.containeridcontainerthreads = containeridcontainerthreads;
		this.containeridports = containeridports;
		this.receivedobject = receivedobject;
		this.zo = zo;
	}

	ClassLoader cl;

	/**
	* Executes the node tasks and return the response.
	*/
	public Object call() {
		try {
			Object deserobj = receivedobject;
			if (deserobj instanceof AllocateContainers ac) {
				List<Integer> ports = new ArrayList<>();
				for (int numport = 0;numport < ac.getNumberofcontainers();numport++) {
					int port = Utils.getRandomPort();
					log.debug("Alloting Port " + port);
					ports.add(port);
				}
				containeridports.put(ac.getJobid(), ports);
				return ports;
			} else if (deserobj instanceof LaunchContainers lc) {
				Map<String, Process> processes = new ConcurrentHashMap<>();
				Map<String, List<Thread>> threads = new ConcurrentHashMap<>();
				List<Integer> ports = new ArrayList<>();
				Process proc;
				var host = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST);
				for (int port = 0;port < lc.getCla().getNumberofcontainers();port++) {
					var cr = lc.getCla().getCr().get(port);
					log.debug("Dispatching chamber {}....", (cr.getPort()));
					proc = processes.get((cr.getPort()) + DataSamudayaConstants.EMPTY);
					if (Objects.isNull(proc)) {
						proc = ContainerLauncher.spawnDataSamudayaContainer((cr.getPort()) + DataSamudayaConstants.EMPTY,
								(String) DataSamudayaProperties.get().get(DataSamudayaConstants.CACHEDISKPATH),
								TaskExecutorRunner.class, proploaderpath, cr, lc.getJobid());
						processes.put((cr.getPort()) + DataSamudayaConstants.EMPTY, proc);
						String hp = host + DataSamudayaConstants.UNDERSCORE + cr.getPort();
					}
					ports.add(cr.getPort());
				}
				processes.keySet().parallelStream()
						.map(prockey -> (Tuple2<String, Process>) Tuple.tuple(prockey, processes.get(prockey)))
						.forEach((Tuple2<String, Process> tuple) -> {
							Thread thr = new Thread() {
								public void run() {
									try {
										log.debug("Printing Container Logs");
										InputStream istr = tuple.v2.getInputStream();
										while (true) {
											log.debug("{}", IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (InterruptedException e) {
										log.warn("Interrupted!", e);
										// Restore interrupted state...
										Thread.currentThread().interrupt();
									} catch (Exception ex) {
										log.error("Unable to Launch Container:", ex);
									}
								}
							};
							thr.start();
							threads.put(tuple.v1, new ArrayList<>());
							threads.get(tuple.v1).add(thr);
							thr = new Thread() {
								public void run() {
									try {
										log.debug("Printing Container Error Logs");
										InputStream istr = tuple.v2.getErrorStream();
										while (true) {
											log.debug("{}", IOUtils.readLines(istr, StandardCharsets.UTF_8));
											Thread.sleep(5000);
										}
									} catch (InterruptedException e) {
										log.warn("Interrupted!", e);
										// Restore interrupted state...
										Thread.currentThread().interrupt();
									} catch (Exception ex) {
										log.error("Unable to Launch Container:", ex);
									}
								}
							};
							thr.start();
							threads.get(tuple.v1).add(thr);
						});
				if(containeridcontainerthreads.containsKey(lc.getJobid())) {
					containeridcontainerthreads.get(lc.getJobid()).putAll(threads);
				} else {
					containeridcontainerthreads.put(lc.getJobid(), threads);
				}
				if(containerprocesses.containsKey(lc.getJobid())) {
					containerprocesses.get(lc.getJobid()).putAll(processes);
				} else {
					containerprocesses.put(lc.getJobid(), processes);
				}
				return ports;
			} else if (deserobj instanceof Resources rsc) {
				var runtime = Runtime.getRuntime();
				rsc.setTotalmemory(runtime.totalMemory());
				rsc.setFreememory(Utils.getTotalAvailablePhysicalMemory());
				rsc.setNumberofprocessors(runtime.availableProcessors());
				rsc.setTotaldisksize(Utils.totaldiskspace());
				rsc.setUsabledisksize(Utils.usablediskspace());
				rsc.setPhysicalmemorysize(Utils.getPhysicalMemory());
				return rsc;
			} else if (deserobj instanceof DestroyContainers dc) {
				log.debug("Destroying the Containers with job id: " + dc.getJobid());
				Map<String, List<Thread>> threads = containeridcontainerthreads.remove(dc.getJobid());
				if (!Objects.isNull(threads)) {
					threads.keySet().stream().map(threads::get).flatMap(thrlist -> thrlist.stream())
							.forEach(thr -> {
								try {
									thr.stop();
								} catch (Exception e) {
									log.warn(DataSamudayaConstants.EMPTY, e);
								}
							});
				}
				Map<String, Process> processes = containerprocesses.remove(dc.getJobid());
				if (!Objects.isNull(processes)) {
					processes.entrySet().stream().forEach(entry -> {
						log.debug("Eradicate the chamber case: " + entry);
						destroyProcess(entry.getKey(), entry.getValue(), dc.getJobid());
					});
				}				
			} else if (deserobj instanceof DestroyContainer dc) {
				log.debug("Destroying the Container with id: " + dc.getJobid());
				Map<String, Process> processes = containerprocesses.get(dc.getJobid());
				if (!Objects.isNull(processes)) {
					String taskexecutorport = dc.getContainerhp().split(DataSamudayaConstants.UNDERSCORE)[1];
					processes.keySet().stream().filter(key -> key.equals(taskexecutorport))
							.map(key -> processes.get(key)).forEach(proc -> {
						log.debug("Eradicate the chamber case: " + proc);
						destroyProcess(taskexecutorport, proc, dc.getJobid());
					});
					processes.remove(taskexecutorport);
				} else {
					containerprocesses.keySet().stream().forEach(key -> {
						containerprocesses.get(key).keySet().stream()
								.filter(port -> port.equals(dc.getContainerhp().split(DataSamudayaConstants.UNDERSCORE)[1]))
								.forEach(port -> {
									Process proc = containerprocesses.get(key).get(port);
									if (nonNull(proc)) {
										log.debug("Eradicate the chamber case: " + proc);
										destroyProcess(port, proc, dc.getJobid());
									}
								});
					});
				}
				Map<String, List<Thread>> threads = containeridcontainerthreads.get(dc.getJobid());
				if (!Objects.isNull(threads)) {
					threads.keySet().stream()
							.filter(key -> key.equals(dc.getContainerhp().split(DataSamudayaConstants.UNDERSCORE)[1]))
							.map(threads::get).flatMap(thrlist -> thrlist.stream())
							.forEach(thr -> {
								try {
									thr.stop();
								} catch (Exception e) {
									log.warn(DataSamudayaConstants.EMPTY, e);
								}
							});
					threads.remove(dc.getContainerhp().split(DataSamudayaConstants.UNDERSCORE)[1]);
				}
			} else if (deserobj instanceof SkipToNewLine stnl) {
				long numberofbytesskipped =
						HDFSBlockUtils.skipBlockToNewLine(hdfs, stnl.lblock, stnl.l, stnl.xrefaddress);
				return numberofbytesskipped;
			}
			return true;
		} catch (Exception ex) {
			log.error("Incomplete task with error", ex);
		}
		return false;
	}

	/**
	* Destroy the task executor via node tasks. 
	* @param port
	* @param proc
	* @param jobid
	*/
	public void destroyProcess(String port, Process proc, String jobid) {
		if (proc.isAlive()) {
			try {
				TaskExecutorShutdown taskExecutorshutdown = new TaskExecutorShutdown();
				log.debug("Initiated eradicating the chamber case: {}",
						DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST) + DataSamudayaConstants.UNDERSCORE
								+ port);
				Utils.getResultObjectByInput(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
						+ DataSamudayaConstants.UNDERSCORE + port, taskExecutorshutdown, jobid);
				log.debug("Intercepting the chamber case conscious for {} ",
						DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST) + DataSamudayaConstants.UNDERSCORE
								+ port);
				while (proc.isAlive()) {
					log.debug("Seeking the chamber case stats {}",
							DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST) + DataSamudayaConstants.UNDERSCORE
									+ port);
					Thread.sleep(500);
				}
				log.debug("The chamber case {} shattered for the port {} ", proc, port);
			} catch (Exception ex) {
				log.error("Destroy failed for the process " + proc, ex);
			}
		}
	}

}
