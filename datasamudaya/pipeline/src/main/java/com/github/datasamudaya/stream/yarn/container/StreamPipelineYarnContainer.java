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
package com.github.datasamudaya.stream.yarn.container;

import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorYarn;
import com.github.datasamudaya.stream.executors.StreamPipelineTaskExecutorYarnSQL;
import com.github.datasamudaya.stream.yarn.appmaster.JobRequest;
import com.github.datasamudaya.stream.yarn.appmaster.JobResponse;

/**
 * 
 * @author Arun
 * The yarn container executor for to process Map Reduce pipelining API.  
 */
public class StreamPipelineYarnContainer extends AbstractIntegrationYarnContainer {

	private Map<String, String> containerprops;
	private ExecutorService executor;
	private static final Log log = LogFactory.getLog(StreamPipelineYarnContainer.class);
	private Map<String, JobStage> jsidjsmap;
	private MindAppmasterServiceClient client;

	/**
	 * Pull the Job to perform MR operation execution requesting 
	 * the Yarn App Master Service. The various Yarn operation What operation
	 * to execute i.e WHATTODO,JOBDONE,JOBFAILED. The various operations response from Yarn App master are
	 * STANDBY,RUNJOB or DIE.
	 */
	@Override
	protected void runInternal() {
		StaticComponentContainer.Modules.exportAllToAll();
		Task task;
		JobRequest request;
		byte[] job = null;
		var containerid = getEnvironment().get(DataSamudayaConstants.SHDP_CONTAINERID);
		executor = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors());
		Semaphore lock = new Semaphore(2);
		try {
			log.info("Initializing Container Properties");
			var prop = new Properties();
			prop.putAll(System.getProperties());
			prop.putAll(containerprops);
			DataSamudayaProperties.put(prop);
			log.info("Initializing Container Properties Completed");
			log.info("Initializing Cache");
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
							DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.CACHEBLOCKS + Utils.getCacheID());
			var inmemorycache = DataSamudayaCache.get();
			log.info("Initializing Cache Completed");
			ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
			while (true) {
				request = new JobRequest();
				request.setState(JobRequest.State.WHATTODO);
				request.setContainerid(containerid);
				request.setTimerequested(System.currentTimeMillis());
				log.debug(request.getTimerequested());
				client = (MindAppmasterServiceClient) getIntegrationServiceClient();
				var response = (JobResponse) client.doMindRequest(request);
				log.debug(containerid + ": Response containerid: " + response);
				if (response == null) {
					sleep(1);
					continue;
				}
				if (response.getJob() != null) {
					request = new JobRequest();
					request.setState(JobRequest.State.RESPONSERECIEVED);
					request.setContainerid(containerid);
					request.setTimerequested(System.currentTimeMillis());
					request.setJob(response.getJob());
					client.doMindRequest(request);
				}
				log.debug(containerid + ": Response containerid: " + response.getContainerid());
				log.debug(containerid + ": Response State: " + response.getState() + " " + response.getResmsg());
				if (response.getState().equals(JobResponse.State.STANDBY)) {
					sleep(1);
					continue;
				} else if (response.getState().equals(JobResponse.State.STOREJOBSTAGE)) {
					job = response.getJob();

					var input = new Input(new ByteArrayInputStream(job));
					var object = Utils.getKryo().readClassAndObject(input);
					this.jsidjsmap = (Map<String, JobStage>) object;
					sleep(1);
				} else if (response.getState().equals(JobResponse.State.RUNJOB)) {
					log.debug(containerid + ": Environment " + getEnvironment());
					executor.execute(() -> {
						try {
							lock.acquire();
						} catch (InterruptedException e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						byte[] jobtoprocess = response.getJob();
						var input = new Input(new ByteArrayInputStream(jobtoprocess));
						var object = Utils.getKryo().readClassAndObject(input);
						Task tasktoprocess = (Task) object;
						StreamPipelineTaskExecutorYarn yarnexecutor = null;
						if (nonNull(tasktoprocess.getStorage()) && tasktoprocess.getStorage() == STORAGE.COLUMNARSQL) {
							yarnexecutor = new StreamPipelineTaskExecutorYarnSQL(
									containerprops.get(DataSamudayaConstants.HDFSNAMENODEURL),
									jsidjsmap.get(tasktoprocess.jobid + tasktoprocess.stageid),
									tasktoprocess.isTopersist());
							yarnexecutor.setCache(inmemorycache);
						} else {
							yarnexecutor = new StreamPipelineTaskExecutorYarn(
									containerprops.get(DataSamudayaConstants.HDFSNAMENODEURL),
									jsidjsmap.get(tasktoprocess.jobid + tasktoprocess.stageid));
							yarnexecutor.setCache(inmemorycache);
						}
						yarnexecutor.setTask(tasktoprocess);
						yarnexecutor.setExecutor(executor);
						yarnexecutor.call();
						JobRequest jr = new JobRequest();
						jr.setState(JobRequest.State.JOBDONE);
						jr.setJob(jobtoprocess);
						jr.setContainerid(containerid);
						JobResponse jresp = (JobResponse) client.doMindRequest(jr);
						log.debug(containerid + ": Task Completed=" + tasktoprocess);
						lock.release();
					});
				} else if (response.getState().equals(JobResponse.State.DIE)) {
					log.debug(containerid + ": Container dies: " + response.getState());
					break;
				}
				log.debug(containerid + ": Response state=" + response.getState());

			}
			log.debug(containerid + ": Completed Job Exiting with status 0...");
			ByteBufferPoolDirect.destroy();
			shutdownExecutor();
			System.exit(0);
		}
		catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		}
		catch (Exception ex) {
			request = new JobRequest();
			request.setState(JobRequest.State.JOBFAILED);
			request.setJob(job);
			if (client != null) {
				var response = (JobResponse) client.doMindRequest(request);
				log.debug("Job Completion Error..." + response.getState() + "..., See cause below \n", ex);
			}
			ByteBufferPoolDirect.destroy();
			try {
				shutdownExecutor();
			} catch (InterruptedException e) {
				log.warn("Interrupted!", e);
				// Restore interrupted state...
				Thread.currentThread().interrupt();
			} catch (Exception e) {
				log.error("", e);
			}
			System.exit(-1);
		}
	}

	public void shutdownExecutor() throws InterruptedException {
		if (executor != null) {
			executor.shutdownNow();
			executor.awaitTermination(1, TimeUnit.SECONDS);
		}
	}

	public Map<String, String> getContainerprops() {
		return containerprops;
	}

	public void setContainerprops(Map<String, String> containerprops) {
		this.containerprops = containerprops;
	}

	private static void sleep(int seconds) {
		try {
			Thread.sleep(1000l * seconds);
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.debug("Delay error, See cause below \n", ex);
		}
	}

}
