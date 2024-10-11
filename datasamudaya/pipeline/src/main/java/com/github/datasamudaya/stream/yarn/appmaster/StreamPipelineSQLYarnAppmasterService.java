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
package com.github.datasamudaya.stream.yarn.appmaster;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author Arun
 * Yarn application master service for stream pipelining jobs API. 
 */
public class StreamPipelineSQLYarnAppmasterService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(StreamPipelineSQLYarnAppmasterService.class);


	private StreamPipelineSQLYarnAppmaster sqlYarnAppMaster;

	public StreamPipelineSQLYarnAppmasterService() {
		StaticComponentContainer.Modules.exportAllToAll();
	}

	/**
	 * Retrieve the jobs request using MindApp Master Deserialiation 
	 * and return the response using the MindApp Master Serialization 
	 * classes configured in appmaster-context.xml.
	 */
	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		var request = getConversionService().convert(message, BaseObject.class);
		var jobrequest = (JobRequest) request;
		log.debug("Request from container: " + jobrequest.getContainerid() + " " + jobrequest.getTimerequested());
		var response = handleJob(jobrequest);
		var mindrpcmessageholder = getConversionService().convert(response, MindRpcMessageHolder.class);
		log.debug("Response to container: " + response.getContainerid() + " :" + mindrpcmessageholder);
		return mindrpcmessageholder;
	}

	public StreamPipelineSQLYarnAppmaster getSQLYarnAppMaster() {
		return sqlYarnAppMaster;
	}

	public void setSQLYarnAppMaster(StreamPipelineSQLYarnAppmaster sqlYarnAppMaster) {
		this.sqlYarnAppMaster = sqlYarnAppMaster;
	}

	/**
	 * Handle the jobs request and return the jobs response.
	 * @param request
	 * @return
	 */
	private JobResponse handleJob(JobRequest request) {
		var response = new JobResponse(JobResponse.State.STANDBY, null);
		response.setResstate(JobResponse.State.RUNJOB.name());
		response.setResmsg("" + request.getTimerequested());
		try {
			if (request.getState().equals(JobRequest.State.GETTASKEXECUTORID)) {
				response.setContainerid(sqlYarnAppMaster.getTaskExecutorId());
				response.setExecutorordriver(sqlYarnAppMaster.getExecutorType());
				return response;
			}
		}
		catch (Exception ex) {
			log.error("Handle job request error, See cause below \n", ex);
		}
		finally {
			log.debug("Response: state=" + response.getState() + "for Job Id "+sqlYarnAppMaster.getTaskExecutorId());
		}
		return response;
	}


}
