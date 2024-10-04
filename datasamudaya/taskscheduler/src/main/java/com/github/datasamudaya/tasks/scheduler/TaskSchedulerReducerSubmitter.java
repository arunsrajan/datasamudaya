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
package com.github.datasamudaya.tasks.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import org.apache.curator.framework.CuratorFramework;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.ReducerValues;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskSchedulerReducerSubmitterMBean;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Task scheduler for reducer.
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class TaskSchedulerReducerSubmitter
		implements TaskSchedulerReducerSubmitterMBean, Callable<Context> {
	private static Logger log = Logger.getLogger(TaskSchedulerReducerSubmitter.class);
	ReducerValues rv;
	String hp;
	String applicationid;
	String stageid;
	String taskid;
	long reducersubmittedcount;
	CuratorFramework cf;
	String executorid;
	boolean iscompleted;

	public TaskSchedulerReducerSubmitter(String currentexecutor, ReducerValues rv, String applicationid, String stageid, String taskid,
			long reducersubmittedcount, CuratorFramework cf, String executorid) {
		this.rv = rv;
		this.hp = currentexecutor;
		this.applicationid = applicationid;
		this.stageid = stageid;
		this.taskid = taskid;
		this.reducersubmittedcount = reducersubmittedcount;
		this.cf = cf;
		this.executorid = executorid;
		iscompleted = false;
	}

	public String getTaskExecutorBalanced(long currentexecutor, List<String> taskexecutors) {

		return taskexecutors.get((int) (currentexecutor % taskexecutors.size()));
	}

	@SuppressWarnings("unchecked")
	@Override
	public Context call() throws Exception {
		List objects = new ArrayList<>();
		objects.add(rv);
		Task task = new Task();
		task.setJobid(applicationid);
		task.setStageid(stageid);
		task.setTaskid(taskid);
		task.setHostport(hp);
		task.setTeid(executorid);
		objects.add(task);
		objects.add(executorid);
		log.debug("Submitting Reducer Task: " + objects);
		return (Context) Utils.getResultObjectByInput(hp, objects, executorid);

	}

	@Override
	public void setHostPort(String hp) {
		this.hp = hp;
	}

	@Override
	public long getReducerSubmittedCount() {
		return this.reducersubmittedcount;
	}

	@Override
	public String getHostPort() {
		return this.hp;
	}

	@Override
	public CuratorFramework getCuratorFramework() {
		return cf;
	}

	@Override
	public String getExecutorid() {
		return executorid;
	}
}
