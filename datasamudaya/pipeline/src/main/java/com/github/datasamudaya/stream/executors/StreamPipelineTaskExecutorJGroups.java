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
package com.github.datasamudaya.stream.executors;

import static java.util.Objects.nonNull;

import java.io.BufferedInputStream;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jgroups.JChannel;

import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author Arun 
 * Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public class StreamPipelineTaskExecutorJGroups extends StreamPipelineTaskExecutor {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorJGroups.class);
	private List<Task> tasks;
	Map<String, JobStage> jsidjsmap;
	public double timetaken = 0.0;
	public JChannel channel;
	private int port;

	public StreamPipelineTaskExecutorJGroups(Map<String, JobStage> jsidjsmap, List<Task> tasks, int port, Cache cache) {
		super(jsidjsmap.get(tasks.get(0).jobid + tasks.get(0).stageid), cache);
		this.jsidjsmap = jsidjsmap;
		this.tasks = tasks;
		this.port = port;
	}
	ExecutorService es;
	
	/**
	 * This method call computes the tasks from stages and return 
	 * whether the tasks are computed successfully.
	 */
	@Override
	public Boolean call() {
		log.debug("Entered MassiveDataStreamJGroupsTaskExecutor.call");
		var taskstatusmap = tasks.parallelStream()
				.map(task -> task.jobid + task.taskid)
				.collect(Collectors.toMap(key -> key, value -> WhoIsResponse.STATUS.YETTOSTART));
		var taskstatusconcmapreq = new ConcurrentHashMap<>(
				taskstatusmap);
		var taskstatusconcmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL);
		String host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		Semaphore semaphore = new Semaphore(Runtime.getRuntime().availableProcessors());
		try (var hdfscompute = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfscompute;
			channel = Utils.getChannelTaskExecutor(jobstage.getTejobid(),
					host,
					port, taskstatusconcmapreq, taskstatusconcmapresp);
			log.info("Work in Jgroups agent: " + tasks + " in host: " + host + " port: " + port);
			var cd = new CountDownLatch(tasks.size());
			var exec = executor;
			for (var tasktocompute : tasks) {
				semaphore.acquire();
				es.submit(new StreamPipelineTaskExecutor(jsidjsmap.get(tasktocompute.jobid + tasktocompute.stageid),
						cache) {
					public Boolean call() {
						hdfs = hdfscompute;
						task = tasktocompute;
						executor = exec;						
						var stagePartition = task.jobid + task.taskid;
						try {
							var taskspredecessor = task.taskspredecessor;
							if (!taskspredecessor.isEmpty()) {
								var taskids = taskspredecessor.parallelStream().map(tk -> tk.jobid + tk.taskid)
										.collect(Collectors.toList());
								var breakloop = false;
								while (true) {
									var tasktatusconcmap = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
									tasktatusconcmap.putAll(taskstatusconcmapresp);
									tasktatusconcmap.putAll(taskstatusconcmapreq);
									breakloop = true;
									for (var taskid : taskids) {
										if (taskstatusconcmapresp.get(taskid) != null && taskstatusconcmapresp
												.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
											Utils.whois(channel, taskid);
											breakloop = false;
											continue;
										} else if (tasktatusconcmap.get(taskid) != null) {
											if (tasktatusconcmap.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
												breakloop = false;
												continue;
											}

										} else {
											Utils.whois(channel, taskid);
											breakloop = false;
											continue;
										}
									}
									if (breakloop) {
										break;
									}
									Thread.sleep(1000);
								}
							}
							log.debug("Submitted Stage " + stagePartition);

							taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.RUNNING);
							if (task.input != null && task.parentremotedatafetch != null) {
								if (task.parentremotedatafetch != null && task.parentremotedatafetch[0] != null) {
									var numinputs = task.parentremotedatafetch.length;
									for (var inputindex = 0;inputindex < numinputs;inputindex++) {
										var input = task.parentremotedatafetch[inputindex];
										log.info("Task Input " + task.jobid + " rdf:" + input);
										if (input != null) {
											var rdf = input;
											InputStream is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(
													rdf.getJobid(), getIntermediateDataRDF(rdf.getTaskid()));
											if (Objects.isNull(is)) {
												RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
												task.input[inputindex] = new ByteArrayInputStream(rdf.getData());
											} else {
												task.input[inputindex] = is;
											}
										}
									}
								} else if (task.input != null && task.input[0] != null) {
									var numinputs = task.input.length;
									for (var inputindex = 0;inputindex < numinputs;inputindex++) {
										var input = task.input[inputindex];
										if (input != null && input instanceof Task taskinput) {
											var os = getIntermediateInputStreamFS(taskinput);
											log.info("Task Input " + taskinput.jobid + " Os:" + os);
											if (os != null) {
												task.input[inputindex] = new BufferedInputStream(os);
											}
										}
									}
								}
							} 

							var timetakenseconds = computeTasks(task, hdfs);
							log.info("Completed Stage " + stagePartition + " in " + timetakenseconds);
							taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.COMPLETED);
						} catch (Exception ex) {
							log.error("Failed Stage " + tasks, ex);
							completed = false;
						} finally {
							semaphore.release();
							cd.countDown();
						}
						return completed;
					}
				});
			}
			log.debug("StagePartitionId with Stage Statuses: " + taskstatusconcmapreq
					+ " WhoIs Response stauses: " + taskstatusconcmapresp);
			cd.await();
			completed = true;			
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Throwable ex) {
			completed = false;
			log.error("Failed Stage: " + task.stageid, ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				endtime = System.currentTimeMillis();
				task.taskstatus = TaskStatus.FAILED;
				task.tasktype = TaskType.EXECUTEUSERTASK;
				task.stagefailuremessage = new String(baos.toByteArray());
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		} finally {
			if(nonNull(es)) {
				es.shutdown();
				try {
					es.awaitTermination(2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("Failed Shutdown executors" + es);
				}
			}
		}
		log.debug("Exiting MassiveDataStreamJGroupsTaskExecutor.call");
		return completed;
	}

}
