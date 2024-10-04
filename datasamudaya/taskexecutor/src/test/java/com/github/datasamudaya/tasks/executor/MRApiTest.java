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
package com.github.datasamudaya.tasks.executor;

import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.lang.reflect.Constructor;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.jooq.lambda.tuple.Tuple4;
import org.jooq.lambda.tuple.Tuple5;
import org.junit.Test;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.CombinerValues;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.ReducerValues;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.stream.StreamPipelineBaseTestCommon;

public class MRApiTest extends StreamPipelineBaseTestCommon {

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testCombiner() throws Exception {
		Combiner<String, Integer, Context> cc = (val, values, context) -> {
			int sum = 0;
			for (Integer value :values) {
				sum += value;
			}
			context.put(val, sum);
		};
		Context<String, Integer> ctx = new DataCruncherContext();
		ctx.put("PS", 100);
		ctx.put("PS", -12100);
		ctx.put("SW", -100);
		ctx.put("SW", -1200);
		Task task = new Task();
		task.setJobid("Job-1");
		task.setStageid("Stage-1");
		task.setTaskid("Task-1");
		CombinerExecutor datasamudayac = new CombinerExecutor(ctx, cc, task);
		Context<String, Integer> result = datasamudayac.call();
		assertEquals(-12000, (int) (result.get("PS").iterator().next()));
		assertEquals(-1300, (int) result.get("SW").iterator().next());
	}

	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	@Test
	public void testMapper() throws Exception {
		Mapper<Long, String, Context> cm = (val, line, context) -> {
			String[] contents = line.split(",");
			if (contents[0] != null && !"Year".equals(contents[0])) {
				if (contents != null && contents.length > 14 && contents[14] != null && !"NA".equals(contents[14])) {
					context.put(contents[8], Integer.parseInt(contents[14]));
				}
			}
		};
		InputStream is = MRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		Task task = new Task();
		task.setJobid("Job-2");
		task.setStageid("Stage-1");
		task.setTaskid("Task-1");
		MapperExecutor datasamudayam = new MapperExecutor(null, lzis, Arrays.asList(cm), task);
		Context<String, Integer> result = datasamudayam.call();
		assertEquals(45957l, (int) (result.get("AQ").size()));
	}


	@SuppressWarnings({"rawtypes", "unchecked", "resource"})
	@Test
	public void testReducer() throws Exception {
		Reducer<String, Integer, Context> cr = (val, values, context) -> {
			int sum = 0;
			for (Integer value :values) {
				sum += value;
			}
			context.put(val, sum);
		};
		DataCruncherContext<String, Integer> dcc = new DataCruncherContext();
		dcc.put("PS", 100);
		dcc.put("PS", -12100);
		dcc.put("SW", -100);
		dcc.put("SW", -1200);
		Task task = new Task();
		task.setJobid("Job-3");
		task.setStageid("Stage-1");
		task.setTaskid("Task-1");
		ReducerExecutor datasamudayac = new ReducerExecutor(dcc, cr, null, task);
		Context<String, Integer> result = datasamudayac.call();
		assertEquals(-12000, (int) (result.get("PS").iterator().next()));
		assertEquals(-1300, (int) result.get("SW").iterator().next());
	}


	@SuppressWarnings({"resource", "rawtypes", "unchecked"})
	@Test
	public void testTaskExecutorMapperCombiner() throws Exception {
		InputStream is = MRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		BlocksLocation bls = new BlocksLocation();
		bls.setMapperclasses(new LinkedHashSet<>(Arrays.asList(new AirlineDataMapper())));
		ExecutorService es = Executors.newWorkStealingPool();
		Task task = new Task();
		task.setJobid("Job-4");
		task.setStageid("Stage-1");
		task.setTaskid("Task-1");
		TaskExecutorMapper mdtemc = new
				TaskExecutorMapper(bls, lzis, task, Thread.currentThread().getContextClassLoader(), 12121);
		mdtemc.call();
		Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
		apptaskexecutormap.put(task.getJobid()+task.getStageid()+task.getTaskid(), mdtemc);
		CombinerValues combinervalues = new CombinerValues();
		combinervalues.setTuples(Arrays.asList(new Tuple5<>("AQ", Arrays.asList(task.getJobid()+task.getStageid()+task.getTaskid()), Arrays.asList("127.0.0.1_1000"),Arrays.asList("1000"), Arrays.asList(task))));
		combinervalues.setAppid(task.getJobid());
		Constructor cons = AirlineDataMapper.class.getDeclaredConstructor();
		combinervalues.setCombiner(cons.newInstance(new Object[cons.getParameterCount()]));
		task = new Task();
		task.setJobid("Job-5");
		task.setStageid("Stage-2");
		task.setTaskid("Task-1");
		TaskExecutorCombiner reducerexec = new TaskExecutorCombiner(combinervalues, task, Thread.currentThread().getContextClassLoader(), 12121, apptaskexecutormap, task.getJobid());
		reducerexec.call();
		Context ctx = (Context) reducerexec.ctx;
		es.shutdown();
		assertEquals(-63278, (long) (ctx.get("AQ").iterator().next()));
	}

	@SuppressWarnings({"resource", "rawtypes", "unchecked"})
	@Test
	public void testTaskExecutorMapperReducer() throws Exception {
		InputStream is = MRApiTest.class.getResourceAsStream("/airlinesample.csv");
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		SnappyOutputStream lzos = new SnappyOutputStream(baos);
		lzos.write(is.readAllBytes());
		lzos.flush();
		SnappyInputStream lzis = new SnappyInputStream(new ByteArrayInputStream(baos.toByteArray()));
		BlocksLocation bls = new BlocksLocation();
		bls.setMapperclasses(new LinkedHashSet<>(Arrays.asList(new AirlineDataMapper())));
		ExecutorService es = Executors.newWorkStealingPool();
		Task task = new Task();
		task.setJobid("Job-5");
		task.setStageid("Stage-1");
		task.setTaskid("Task-1");
		TaskExecutorMapper mdtemc = new
				TaskExecutorMapper(bls, lzis, task, Thread.currentThread().getContextClassLoader(), 12121);
		mdtemc.call();
		Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
		apptaskexecutormap.put(task.getJobid()+task.getStageid()+task.getTaskid(), mdtemc);
		ReducerValues reducervalues = new ReducerValues();
		reducervalues.setTuples(Arrays.asList(new Tuple4<>("AQ", Arrays.asList(task.getJobid()+task.getStageid()+task.getTaskid()), Arrays.asList("127.0.0.1_1000"), Arrays.asList(task))));
		reducervalues.setAppid(task.getJobid());
		Constructor cons = AirlineDataMapper.class.getDeclaredConstructor();
		reducervalues.setReducer(cons.newInstance(new Object[cons.getParameterCount()]));
		task = new Task();
		task.setJobid("Job-5");
		task.setStageid("Stage-2");
		task.setTaskid("Task-1");
		TaskExecutorReducer reducerexec = new TaskExecutorReducer(reducervalues, task, Thread.currentThread().getContextClassLoader(), 12121, apptaskexecutormap, task.getJobid());
		reducerexec.call();
		Context ctx = (Context) reducerexec.ctx;
		es.shutdown();
		assertEquals(-63278, (long) (ctx.get("AQ").iterator().next()));
	}
}
