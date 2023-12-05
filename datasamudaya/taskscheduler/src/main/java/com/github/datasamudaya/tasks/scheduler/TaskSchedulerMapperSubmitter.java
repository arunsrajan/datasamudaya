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
import java.io.IOException;
import java.io.PrintWriter;
import java.net.UnknownHostException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Set;
import org.apache.curator.framework.CuratorFramework;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.ApplicationTask;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.RetrieveKeys;
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
	CuratorFramework cf;
	String hostport[];
	String executorid;
	Boolean iscompleted;

	TaskSchedulerMapperSubmitter(Object blockslocation, boolean mapper, Set<Object> mapperclasses,
			ApplicationTask apptask, CuratorFramework cf, String executorid) {
		this.blockslocation = (BlocksLocation) blockslocation;
		this.mapper = mapper;
		this.mapperclasses = mapperclasses;
		this.apptask = apptask;
		this.cf = cf;
		this.executorid = executorid;
	}

	public BlocksLocation initializeobject(Set<Object> mapperclasses, Set<Object> combinerclasses)
			throws Exception, UnknownHostException, IOException {
		this.hostport = blockslocation.getExecutorhp().split(DataSamudayaConstants.UNDERSCORE);
		blockslocation.setMapperclasses(mapperclasses);
		blockslocation.setCombinerclasses(combinerclasses);
		return blockslocation;
	}

	public RetrieveKeys sendChunk(BlocksLocation blockslocation) throws Exception {
		try {
			var objects = new ArrayList<>();
			objects.add(blockslocation);
			objects.add(apptask.getApplicationid());
			objects.add(apptask.getTaskid());
			var baos = new ByteArrayOutputStream();
			var output = new Output(baos);
			Utils.getKryoInstance().writeClassAndObject(output, objects);
			output.flush();
			output.close();
			return (RetrieveKeys) Utils.getResultObjectByInput(blockslocation.getExecutorhp(), baos.toByteArray(), executorid);
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
	public CuratorFramework getCuratorFramework() {
		return cf;
	}

	@Override
	public String getExecutorid() {
		return executorid;
	}
}
