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
import java.io.ByteArrayOutputStream;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.yarn.integration.ip.mind.MindAppmasterService;
import org.springframework.yarn.integration.ip.mind.MindRpcMessageHolder;
import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author Arun
 * Yarn application master service for stream pipelining jobs API. 
 */
public class MapReduceYarnAppmasterService extends MindAppmasterService {

	private static final Log log = LogFactory.getLog(MapReduceYarnAppmasterService.class);


	private MapReduceYarnAppmaster yarnAppMaster;

	/**
	 * Retrieve the jobs request using MindApp Master Deserialiation 
	 * and return the response using the MindApp Master Serialization 
	 * classes configured in appmaster-context.xml.
	 */
	@Override
	protected MindRpcMessageHolder handleMindMessageInternal(MindRpcMessageHolder message) {
		var request = getConversionService().convert(message, BaseObject.class);
		var jobrequest = (JobRequest) request;
		log.info("Request from container: " + jobrequest.getContainerid() + " " + jobrequest.getTimerequested());
		var response = handleJob(jobrequest);
		var mindrpcmessageholder = getConversionService().convert(response, MindRpcMessageHolder.class);
		log.info("Response to container: " + response.getContainerid() + " :" + mindrpcmessageholder);
		return mindrpcmessageholder;
	}

	public MapReduceYarnAppmaster getYarnAppMaster() {
		return yarnAppMaster;
	}

	public void setYarnAppMaster(MapReduceYarnAppmaster yarnAppMaster) {
		this.yarnAppMaster = yarnAppMaster;
	}

	/**
	 * Handle the jobs request and return the jobs response.
	 * @param request
	 * @return
	 */
	private JobResponse handleJob(JobRequest request) {
		var response = new JobResponse(JobResponse.State.STANDBY, null);
		response.setResstate(JobResponse.State.STANDBY.name());
		response.setResmsg("" + request.getTimerequested());
		response.setContainerid(request.getContainerid());
		try {
			if (request.getJob() != null) {
				try (var input = new Input(new ByteArrayInputStream(request.getJob()));) {
					var object = Utils.getKryo().readClassAndObject(input);
					if (object instanceof MapperCombiner mc) {
						// Update statuses to App Master if job has been completed.
						if (request.getState().equals(JobRequest.State.JOBDONE)) {
							yarnAppMaster.reportJobStatus(mc, true, request.getContainerid());
							return response;
						}
						// Update statuses to App Master if job has been Failed.
						else if (request.getState().equals(JobRequest.State.JOBFAILED)) {
							yarnAppMaster.reportJobStatus(mc, false, request.getContainerid());
						}
					}
					else if (object instanceof YarnReducer red) {
						// Update statuses to App Master if job has been completed.
						if (request.getState().equals(JobRequest.State.JOBDONE)) {
							yarnAppMaster.reportJobStatus(red, true, request.getContainerid());
							return response;
						}
						// Update statuses to App Master if job has been Failed.
						else if (request.getState().equals(JobRequest.State.JOBFAILED)) {
							yarnAppMaster.reportJobStatus(red, false, request.getContainerid());
						}
					}
				} catch (Exception ex) {
					log.info("Handle job request error, See cause below \n", ex);
				}
			}

			var job = yarnAppMaster.getJob(request.getContainerid());
			log.info(request.getContainerid() + ": " + job);
			//Job is available
			if (job != null) {
				var baos = new ByteArrayOutputStream();
				var output = new Output(baos);
				Utils.getKryo().writeClassAndObject(output, job);
				output.flush();
				output.close();
				response.setJob(baos.toByteArray());
				response.setState(JobResponse.State.RUNJOB);
				response.setResstate(JobResponse.State.RUNJOB.name());
			}
			//If there is no jobs to executor return the status for
			//container to DIE.
			else if (!yarnAppMaster.hasJobs()) {
				response.setState(JobResponse.State.DIE);
				response.setResstate(JobResponse.State.DIE.name());
			}
		}
		catch (Exception ex) {
			log.error("Handle job request error, See cause below \n", ex);
		}
		finally {
			log.info("Response: state=" + response.getState() + " job=" + response.getJob());
		}
		return response;
	}


}
