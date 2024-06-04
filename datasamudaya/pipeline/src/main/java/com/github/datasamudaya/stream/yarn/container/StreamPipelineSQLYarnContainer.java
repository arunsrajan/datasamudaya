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

import java.lang.reflect.Method;
import java.net.InetAddress;
import java.util.Map;

import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.yarn.integration.container.AbstractIntegrationYarnContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterServiceClient;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.EXECUTORTYPE;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.yarn.appmaster.JobRequest;
import com.github.datasamudaya.stream.yarn.appmaster.JobResponse;

/**
 * 
 * @author Arun
 * The yarn container executor for to process Map Reduce pipelining API.  
 */
public class StreamPipelineSQLYarnContainer extends AbstractIntegrationYarnContainer {
	private static final Logger log = LoggerFactory.getLogger(StreamPipelineSQLYarnContainer.class);
	private Map<String, String> containerprops;
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
		JobRequest request;
		try {
			log.info("In runInternal method started with environment {}", getEnvironment());
			Class<?> cls = Class.forName(DataSamudayaConstants.DEFAULTASKEXECUTORRUNNER, true, getClass().getClassLoader());
			Method method = cls.getDeclaredMethod("main", String[].class);
			log.info("In Main method Obtained");
			System.setProperty(DataSamudayaConstants.TASKEXECUTOR_HOST, getEnvironment(DataSamudayaConstants.NM_HOST));
			System.setProperty(DataSamudayaConstants.TASKEXECUTOR_PORT, DataSamudayaConstants.EMPTY
					+ Utils.getRandomPort());
			request = new JobRequest();
			request.setState(JobRequest.State.GETTASKEXECUTORID);
			log.info("In Client Request Set");
			client = (MindAppmasterServiceClient) getIntegrationServiceClient();
			var response = (JobResponse) client.doMindRequest(request);
			log.info("In Client Response Obtained");
			String jobid = response.getContainerid();
			log.info("JobId {}", jobid);
			String[] args = new String[]{DataSamudayaConstants.TEPROPLOADCLASSPATHCONFIG, ((int)(Utils.mpBeanLocalToJVM.getUsage().getMax() * 0.4)) +"", jobid, EXECUTORTYPE.EXECUTOR.name()};  
			method.invoke(null, (Object) args);
			log.info("Executing TaskExecutor For JobId {}", jobid);
			log.info("In runInternal method Ended");
			System.exit(-1);
		}
		catch (Throwable ex) {
			log.error("In runInternal method Ended", ex);
		}
	}
	
	public Map<String, String> getContainerprops() {
		return containerprops;
	}

	public void setContainerprops(Map<String, String> containerprops) {
		this.containerprops = containerprops;
	}

}
