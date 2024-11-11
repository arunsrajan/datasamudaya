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

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple4;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.ReducerValues;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingContext;
import com.github.datasamudaya.common.utils.IteratorType;
import com.github.datasamudaya.common.utils.RemoteIteratorClient;
import com.github.datasamudaya.common.utils.RequestType;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Task executor for reducer.
 * @author arun
 *
 */
public class TaskExecutorReducer implements Callable<Context> {
	static Logger log = Logger.getLogger(TaskExecutorReducer.class);
	@SuppressWarnings("rawtypes")
	Reducer cr;
	ReducerValues rv;
	File file;
	@SuppressWarnings("rawtypes")
	Context ctx;
	String executorid;
	Task task;
	int port;
	Map<String, Object> apptaskexecutormap;

	@SuppressWarnings({"rawtypes"})
	public TaskExecutorReducer(ReducerValues rv, Task task, ClassLoader cl,
			int port, Map<String, Object> apptaskexecutormap, String executorid) throws Exception {
		this.rv = rv;
		Class<?> clz = null;
		this.port = port;
		try {
			cr = (Reducer) rv.getReducer();
			this.executorid = executorid;
			this.task = task;
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
			log.debug("Submitted Reducer:" + task.getJobid() + task.getTaskid());
			var complete = new DataCruncherContext();
			var apptaskcontextmap = new ConcurrentHashMap<String, DiskSpillingContext>();
			DiskSpillingContext currentctx;
			var cdl = new CountDownLatch(rv.getTuples().size());
			for (var tuple4 : (List<Tuple4>) rv.getTuples()) {
				var ctx = new DataCruncherContext();				
				int index = 0;
				for (var appstgtaskids : (Collection<String>) tuple4.v2) {
					Task remotetask = ((List<Task>)tuple4.v4).get(index);
					remotetask.setHostport((String) ((List)tuple4.v3).get(index));
					if (apptaskcontextmap.get(tuple4.v1.toString() + appstgtaskids) != null) {
						currentctx = apptaskcontextmap.get(tuple4.v1.toString() + appstgtaskids);
						if(currentctx.isSpilled()) {
							Utils.copySpilledContextToDestination(currentctx, Arrays.asList(ctx), tuple4.v1, remotetask, false);
						} else {
							ctx.addAll(tuple4.v1, currentctx.get(tuple4.v1));
						}
					} else {
						Object object =
								(Object) apptaskexecutormap.get(appstgtaskids);
						if (object == null) {
							currentctx = new DiskSpillingContext(task, tuple4.v1.toString() + appstgtaskids);
							Utils.copySpilledContextToDestination(null, Arrays.asList(ctx,currentctx), tuple4.v1, remotetask, true);													
						}
						else if(object instanceof TaskExecutorMapper tem) {
							currentctx = (DiskSpillingContext) tem.ctx;
							if(currentctx.isSpilled()) {
								Utils.copySpilledContextToDestination(currentctx, Arrays.asList(ctx), tuple4.v1, remotetask, false);
							} else {
								ctx.addAll(tuple4.v1, currentctx.get(tuple4.v1));
							}
						} else if(object instanceof TaskExecutorCombiner tec){
							currentctx = (DiskSpillingContext) tec.ctx;
							if(currentctx.isSpilled()) {
								Utils.copySpilledContextToDestination(currentctx, Arrays.asList(ctx), tuple4.v1, remotetask, false);
							} else {
								ctx.addAll(tuple4.v1, currentctx.get(tuple4.v1));
							}
						} else {
							currentctx = null;
						}
						apptaskcontextmap.put(tuple4.v1.toString() + appstgtaskids, currentctx);
					}					
					index++;
				}
				var datasamudayar = new ReducerExecutor(ctx, cr, tuple4.v1, task);
				final var fc = es.submit(datasamudayar);
				esresult.execute(() -> {
					DiskSpillingContext results;
					try {
						lock.acquire();
						results = (DiskSpillingContext) fc.get();
						if(results.isSpilled()) {
							results.close();
							results.keys().forEach(key->
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
			ctx = complete;
			log.debug("Submitted Reducer Completed:" + task.getJobid() + task.getTaskid());
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
			if(esresult != null) {
				esresult.shutdown();
			}
		}
		return ctx;
	}

}
