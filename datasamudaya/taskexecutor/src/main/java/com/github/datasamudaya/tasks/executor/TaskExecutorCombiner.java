/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;

import org.jooq.lambda.tuple.Tuple5;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.CombinerValues;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingContext;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Task executor for reducer.
 * @author arun
 *
 */
public class TaskExecutorCombiner implements Callable<Context> {
	static Logger log = LoggerFactory.getLogger(TaskExecutorCombiner.class);
	@SuppressWarnings("rawtypes")
	Combiner combiner;
	CombinerValues cv;
	File file;
	@SuppressWarnings("rawtypes")
	Context ctx;
	Task task;
	String executorid;
	int port;
	Map<String, Object> apptaskexecutormap;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutorCombiner(CombinerValues cv, Task task, ClassLoader cl,
			int port, Map<String, Object> apptaskexecutormap, String executorid) throws Exception {
		this.cv = cv;
		Class<?> clz = null;
		this.port = port;
		try {
			combiner = (Combiner) cv.getCombiner();
			this.task = task;
			this.executorid = executorid;
		} catch (Exception ex) {
			log.debug("Exception in loading class:", ex);
		}
		this.apptaskexecutormap = apptaskexecutormap;
	}

	/**
	* Executes the reducer tasks and returns the context object. 
	*/
	@SuppressWarnings({"rawtypes", "unchecked"})
	@Override
	public Context call() {
		var es = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE,
						DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), Thread.ofVirtual().factory());
		var esresult = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
				.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE,
						DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), Thread.ofVirtual().factory());
		final var lock = new Semaphore(Runtime.getRuntime().availableProcessors());
		try {
			log.debug("Submitted Combiner:" + task.getJobid() + task.getTaskid());
			var complete = new DiskSpillingContext(task, null);
			var appstgtaskcontextmap = new ConcurrentHashMap<String, Context>();
			DiskSpillingContext currentctx = null;
			var cdl = new CountDownLatch(cv.getTuples().size());
			for (var tuple5 : (List<Tuple5>) cv.getTuples()) {
				var ctx = new DataCruncherContext();
				int index = 0;
				for (var appstgtaskids : (Collection<String>) tuple5.v2) {
					Task remotetask = ((List<Task>) tuple5.v5).get(index);
					remotetask.setHostport((String) ((List) tuple5.v3).get(index));
					if (appstgtaskcontextmap.get(appstgtaskids) != null) {
						currentctx = (DiskSpillingContext) appstgtaskcontextmap.get(tuple5.v1.toString() + appstgtaskids);
						if (currentctx.isSpilled()) {
							Utils.copySpilledContextToDestination(currentctx, Arrays.asList(ctx), tuple5.v1, remotetask, false);
						} else {
							ctx.addAll(tuple5.v1, currentctx.get(tuple5.v1));
						}
					} else {
						TaskExecutorMapper temc =
								(TaskExecutorMapper) apptaskexecutormap.get(appstgtaskids);
						if (temc == null) {
							log.debug("Mapper Task Is Remote To TE");
							currentctx = new DiskSpillingContext(task, tuple5.v1.toString() + appstgtaskids);
							Utils.copySpilledContextToDestination(null, Arrays.asList(ctx, currentctx), tuple5.v1, remotetask, true);
						} else {
							log.debug("Mapper Task Is Local To TE");
							currentctx = (DiskSpillingContext) temc.ctx;
							log.debug("Mapper Task Is Local To TE Is Spilled {}", currentctx.isSpilled());
							if (currentctx.isSpilled()) {
								Utils.copySpilledContextToDestination(currentctx, Arrays.asList(ctx), tuple5.v1, remotetask, false);
							} else {
								log.debug("Size Of Unspilled Data {}", currentctx.get(tuple5.v1).size());
								ctx.addAll(tuple5.v1, currentctx.get(tuple5.v1));
							}
							log.debug("Combiner Task Is Local To TE Completed");
						}
						appstgtaskcontextmap.put(tuple5.v1.toString() + appstgtaskids, currentctx);
					}
					index++;
				}
				var datasamudayar = new CombinerExecutor(ctx, combiner, task);
				final var fc = es.submit(datasamudayar);
				esresult.execute(() -> {
					DiskSpillingContext results;
					try {
						lock.acquire();
						results = (DiskSpillingContext) fc.get();
						if (results.isSpilled()) {
							results.keys().forEach(key ->
									Utils.copySpilledContextToDestination(results, Arrays.asList(complete), key, task, false));
						} else {
							complete.add(results);
						}
					} catch (Exception e) {
						log.error("Send Message Error For Task Failed: ", e);
					} finally {
						cdl.countDown();
						lock.release();
					}
				});
			}
			cdl.await();
			if (complete.isSpilled()) {
				complete.close();
			}
			log.debug("Combiner Result Keys {}", complete.keys());
			ctx = complete;
			log.debug("Submitted Reducer Completed:" + task.getJobid() + task.getStageid() + task.getTaskid());
		} catch (Throwable ex) {
			try {
				var baos = new ByteArrayOutputStream();
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
			} catch (Exception e) {
				log.error("Send Message Error For Task Failed: ", e);
			}
			log.error("Submitted Reducer Failed:", ex);
		} finally {
			if (es != null) {
				es.shutdown();
			}
			if (esresult != null) {
				esresult.shutdown();
			}
		}
		return ctx;
	}

}
