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

import java.io.ByteArrayOutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;

import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.RetrieveKeys;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskSchedulerMapperSubmitterMBean;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Task scheduler for mapper.
 * @author arun
 *
 */
public class TaskSchedulerMapperSubmitter implements TaskSchedulerMapperSubmitterMBean {
	BlocksLocation blockslocation;
	boolean mapper;
	Set<Object> mapperclasses;
	ApplicationTask apptask;
	String executorid;
	Boolean iscompleted;

	TaskSchedulerMapperSubmitter(Object blockslocation, boolean mapper, Set<Object> mapperclasses,
			ApplicationTask apptask, String executorid) {
		this.blockslocation = (BlocksLocation) blockslocation;
		this.mapper = mapper;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.executorid = executorid;
		this.blockslocation.setMapperclasses(mapperclasses);
	}

	public RetrieveKeys execute() throws Exception {
		try {
			var objects = new ArrayList<>();
			objects.add(blockslocation);
			Task task = new Task();
			task.setJobid(apptask.getApplicationid());
			task.setStageid(apptask.getStageid());
			task.setTaskid(apptask.getTaskid());
			task.setHostport(apptask.getHp());
			task.setTeid(executorid);
			objects.add(task);
			objects.add(executorid);
			return (RetrieveKeys) Utils.getResultObjectByInput(blockslocation.getExecutorhp(), objects, executorid);
		}
		catch (Exception ex) {
			var baos = new ByteArrayOutputStream();
			var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
			ex.printStackTrace(failuremessage);
			this.iscompleted = false;
		}
		return null;
	}

	@Override
	public void setHostPort(String hp) {
		blockslocation.setExecutorhp(hp);
	}

	@Override
	public String getHostPort() {
		return blockslocation.getExecutorhp();
	}

	@Override
	public String getExecutorid() {
		return executorid;
	}
}
