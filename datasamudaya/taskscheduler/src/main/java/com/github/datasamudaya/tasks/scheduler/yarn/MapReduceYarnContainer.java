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
package com.github.datasamudaya.tasks.scheduler.yarn;

import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jooq.lambda.tuple.Tuple2;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.executor.Reducer;
import com.github.datasamudaya.tasks.yarn.executor.MapperCombinerExecutor;
import com.github.datasamudaya.tasks.yarn.executor.ReducerExecutor;

/**
 * 
 * @author Arun The yarn container executor for to process Map Reduce pipelining
 *         API.
 */
public class MapReduceYarnContainer extends AbstractIntegrationYarnContainer {

	private Map<String, String> containerprops;

	private static final Log log = LogFactory.getLog(MapReduceYarnContainer.class);


	/**
	 * Pull the Job to perform MR operation execution requesting the Yarn App Master
	 * Service. The various Yarn operation What operation to execute i.e
	 * WHATTODO,JOBDONE,JOBFAILED. The various operations response from Yarn App
	 * master are STANDBY,RUNJOB or DIE.
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	protected void runInternal() {
		StaticComponentContainer.Modules.exportAllToAll();
		log.debug("Container Started...");
		JobRequest request;
		byte[] job = null;
		var containerid = getEnvironment().get(DataSamudayaConstants.SHDP_CONTAINERID);
		MindAppmasterServiceClient client = null;
		try {
			var prop = new Properties();
			DataSamudayaProperties.put(prop);
			ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
			while (true) {
				request = new JobRequest();
				request.setState(JobRequest.State.WHATTODO);
				request.setContainerid(containerid);
				request.setTimerequested(System.currentTimeMillis());
				client = (MindAppmasterServiceClient) getIntegrationServiceClient();
				var response = (JobResponse) client.doMindRequest(request);
				log.debug(containerid + ": Response containerid: " + response);
				if (response == null) {
					sleep(1);
					continue;
				}
				log.debug(containerid + ": Response State: " + response.getState() + " " + response.getResmsg());
				if (response.getState().equals(JobResponse.State.STANDBY)) {
					sleep(1);
					continue;
				} else if (response.getState().equals(JobResponse.State.RUNJOB)) {
					log.debug(containerid + ": Environment " + getEnvironment());
					job = response.getJob();
					var input = new Input(new ByteArrayInputStream(job));
					var object = Utils.getKryo().readClassAndObject(input);
					if (object instanceof MapperCombiner mc) {
						System.setProperty(DataSamudayaConstants.HDFSNAMENODEURL,
								containerprops.get(DataSamudayaConstants.HDFSNAMENODEURL));
						var cm = new ArrayList<Mapper>();
						var cc = new ArrayList<Combiner>();
						prop.putAll(containerprops);
						try (var hdfs = FileSystem.newInstance(
								new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
								new Configuration());) {
							Class<?> clz = null;
							if (mc.mapperclasses != null) {
								for (var mapperclass : mc.mapperclasses) {
									cm.add((Mapper) mapperclass);
								}
							}
							if (mc.combinerclasses != null) {
								for (var combinerclass : mc.combinerclasses) {
									cc.add((Combiner) combinerclass);
								}
							}

							var es = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
									.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE, 
											DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), Thread.ofVirtual().factory());
							var datasamudayamc = new MapperCombinerExecutor(
									mc.blockslocation, CacheUtils.getBlockData(mc.blockslocation, hdfs), cm, cc);
							var fc = (Future<Context>) es.submit(datasamudayamc);
							var ctx = fc.get();
							es.shutdown();
							RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, mc.apptask.getApplicationid(),
									(mc.apptask.getApplicationid() + mc.apptask.getTaskid()));
							ctx = null;
							request = new JobRequest();
							request.setState(JobRequest.State.JOBDONE);
							request.setJob(job);
							request.setContainerid(containerid);
							response = (JobResponse) client.doMindRequest(request);
							log.debug(containerid + ": Task Completed=" + mc);
							sleep(1);
						}
					} else if (object instanceof YarnReducer red) {
						var cr = (Reducer) red.reducerclasses.iterator().next();
						var complete = new DataCruncherContext();
						var apptaskcontextmap = new ConcurrentHashMap<String, Context>();
						Context currentctx;
						var es = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
								.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE, 
										DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), Thread.ofVirtual().factory());
						for (var tuple2 : (List<Tuple2>) red.tuples) {
							var ctx = new DataCruncherContext();
							for (var apptaskids : (Collection<String>) tuple2.v2) {
								if (apptaskcontextmap.get(apptaskids) != null) {
									currentctx = apptaskcontextmap.get(apptaskids);
								} else {
									currentctx = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(red.apptask.getApplicationid(),
											apptaskids, false);
									apptaskcontextmap.put(apptaskids, currentctx);
								}
								ctx.addAll(tuple2.v1, currentctx.get(tuple2.v1));
							}
							log.debug("In Reducer ctx: " + ctx);
							var datasamudayar = new ReducerExecutor((DataCruncherContext) ctx, cr,
									tuple2.v1);
							var fc = (Future<Context>) es.submit(datasamudayar);
							Context results = fc.get();
							complete.add(results);
							log.debug("Complete Result: " + complete);
						}
						RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(complete, red.apptask.getApplicationid(),
								(red.apptask.getApplicationid() + red.apptask.getTaskid()));
						es.shutdown();
						request = new JobRequest();
						request.setState(JobRequest.State.JOBDONE);
						request.setJob(job);
						request.setContainerid(containerid);
						response = (JobResponse) client.doMindRequest(request);
						log.debug(containerid + ": Task Completed=" + red);
						sleep(1);
					}
				} else if (response.getState().equals(JobResponse.State.DIE)) {
					log.debug(containerid + ": Container dies: " + response.getState());
					break;
				}
				log.debug(containerid + ": Response state=" + response.getState());

			}
			log.debug(containerid + ": Completed Job Exiting with status 0...");
			ByteBufferPoolDirect.destroyPool();
			System.exit(0);
		} catch (Exception ex) {
			request = new JobRequest();
			request.setContainerid(containerid);
			request.setState(JobRequest.State.JOBFAILED);
			request.setJob(job);
			if (client != null) {
				JobResponse response = (JobResponse) client.doMindRequest(request);
				log.debug("Job Completion Error..." + response.getState() + "..., See cause below \n", ex);
			}
			ByteBufferPoolDirect.destroyPool();
			System.exit(-1);
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
