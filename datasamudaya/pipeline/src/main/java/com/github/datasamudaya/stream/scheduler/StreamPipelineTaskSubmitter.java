/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.stream.scheduler;

import java.rmi.registry.LocateRegistry;
import java.rmi.registry.Registry;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.stream.Collectors;

import org.apache.commons.collections.CollectionUtils;
import org.apache.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.ExecuteTaskActor;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.StreamPipelineTaskSubmitterMBean;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.Utils;

import static java.util.Objects.nonNull;

/**
 * The task scheduler thread for submitting tasks using jobstage object.
 * @author Arun 
 */
public class StreamPipelineTaskSubmitter implements StreamPipelineTaskSubmitterMBean, Callable<Object> {

	static Logger log = Logger.getLogger(StreamPipelineTaskSubmitter.class);
	private int level;
	private Task task;
	private String hp;
	private boolean completedexecution;
	private boolean resultobtainedte;
	private PipelineConfig pc;
	private List<String> childactors;
	private List<String> taskexecutors;

	public StreamPipelineTaskSubmitter() {
	}

	public boolean isCompletedexecution() {
		return completedexecution;
	}

	public void setCompletedexecution(boolean completedexecution) {
		this.completedexecution = completedexecution;
	}

	public StreamPipelineTaskSubmitter(Task task, String hp, PipelineConfig pc) {
		this.task = task;
		this.hp = hp;
		this.pc = pc;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}

	/**
	 * Submit the job stages to task exectuors.
	 */
	@Override
	public Object call() throws Exception {
		try {
			String jobid = task.jobid;
			if (nonNull(pc) && pc.getUseglobaltaskexecutors()) {
				jobid = pc.getTejobid();
			}
			String hostport[] = hp.split(DataSamudayaConstants.UNDERSCORE);
			Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher sdc = (StreamDataCruncher) registry.lookup(DataSamudayaConstants.BINDTESTUB
					+ DataSamudayaConstants.HYPHEN + jobid);
			return sdc.postObject(task);
		} catch (Exception ex) {
			log.error("Unable to connect and submit tasks to executor with host and port: " + hp, ex);
			throw ex;
		}
	}

	/**
	 * Execute Akka Actors
	 * @return result
	 * @throws Exception
	 */
	public Object actors() throws Exception {
		try {
			String jobid = task.jobid;
			if (nonNull(pc) && pc.getUseglobaltaskexecutors()) {
				jobid = pc.getTejobid();
			}
			String hostport[] = hp.split(DataSamudayaConstants.UNDERSCORE);
			Registry registry = LocateRegistry.getRegistry(hostport[0], Integer.parseInt(hostport[1]));
			StreamDataCruncher sdc = (StreamDataCruncher) registry.lookup(DataSamudayaConstants.BINDTESTUB
					+ DataSamudayaConstants.HYPHEN + jobid);
			if (CollectionUtils.isNotEmpty(task.getShufflechildactors())) {
				childactors = task.getShufflechildactors().stream().map(task -> task.actorselection).collect(Collectors.toList());
			}
			ExecuteTaskActor eta = new ExecuteTaskActor(task, childactors, taskexecutors.indexOf(hostport) * 3);
			byte[] objbytes = Utils.convertObjectToBytes(eta);
			return sdc.postObject(objbytes);
		} catch (Exception ex) {
			log.error("Unable to connect and submit tasks to executor with host and port: " + hp, ex);
			throw ex;
		}
	}

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + (task == null ? 0 : task.hashCode());
		return result;
	}

	@Override
	public boolean equals(Object obj) {
		if (this == obj) {
			return true;
		}
		if (obj == null) {
			return false;
		}
		if (getClass() != obj.getClass()) {
			return false;
		}
		StreamPipelineTaskSubmitter other = (StreamPipelineTaskSubmitter) obj;
		if (hp == null) {
			if (other.hp != null) {
				return false;
			}
		} else if (!hp.equals(other.hp)) {
			return false;
		}
		if (task == null) {
			if (other.task != null) {
				return false;
			}
		} else if (!task.equals(other.task)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "StreamPipelineTaskSubmitter [task=" + task + "]";
	}

	@Override
	public void setHostPort(String hp) {
		this.hp = hp;

	}

	@Override
	public String getHostPort() {
		return hp;
	}

	public int getLevel() {
		return level;
	}

	public void setLevel(int level) {
		this.level = level;
	}

	public boolean isResultobtainedte() {
		return resultobtainedte;
	}

	public void setResultobtainedte(boolean resultobtainedte) {
		this.resultobtainedte = resultobtainedte;
	}

	public PipelineConfig getPc() {
		return pc;
	}

	public void setPc(PipelineConfig pc) {
		this.pc = pc;
	}

	public List<String> getChildactors() {
		return childactors;
	}

	public void setChildactors(List<String> childactors) {
		this.childactors = childactors;
	}

	public void setTaskexecutors(List<String> taskexecutors) {
		this.taskexecutors = taskexecutors;
	}

}
