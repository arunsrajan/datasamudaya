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

import org.springframework.yarn.integration.ip.mind.binding.BaseObject;

/**
 * 
 * @author Arun
 * Job request object Hadoop yarn App Master and Yarn Container for MR pipelining API.
 */
public class JobRequest extends BaseObject {

	private State state;
	private byte[] job;
	private String containerid;
	private long timerequested;

	public JobRequest() {
	}

	public JobRequest(State state, byte[] job) {
		super();
		this.state = state;
		this.job = job;
	}

	public State getState() {
		return state;
	}

	public void setState(State state) {
		this.state = state;
	}

	public byte[] getJob() {
		return job;
	}

	public void setJob(byte[] job) {
		this.job = job;
	}

	public String getContainerid() {
		return containerid;
	}

	public void setContainerid(String containerid) {
		this.containerid = containerid;
	}

	public long getTimerequested() {
		return timerequested;
	}

	public void setTimerequested(long timerequested) {
		this.timerequested = timerequested;
	}

	public enum State {
		WHATTODO,
		JOBDONE,
		JOBFAILED,
		RESPONSERECIEVED,
		GETTASKEXECUTORID
	}

}
