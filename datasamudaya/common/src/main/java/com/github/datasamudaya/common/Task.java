/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.io.Serializable;
import java.util.List;
import java.util.Objects;

import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 * @author Arun 
 * Holds the task information such as execution function, parent and child tasks in the
 * form graph for the streaming API.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class Task implements Serializable, Cloneable {
  private static final long serialVersionUID = 4608751332110497234L;
  public Object[] input;
  public RemoteDataFetch[] parentremotedatafetch;
  public TaskType tasktype;
  public TaskStatus taskstatus;
  public boolean visited;
  public String jobid;
  public String stageid;
  public String taskid;
  public String hostport;
  public String stagefailuremessage;
  public double timetakenseconds;
  private String taskname;
  public STORAGE storage;
  public List<Task> taskspredecessor;
  public boolean finalphase;
  public String hdfsurl;
  public String filepath;
  public boolean saveresulttohdfs;
  public Long taskexecutionstartime;
  public Long taskexecutionendtime;
  public String hbphysicaladdress;
  public String piguuid;
  public long numbytesprocessed;
  public long numbytesgenerated;
  public long numbytesconverted;
  public boolean topersist;
  public String actorselection;
  public String joinpos;
  
  @Override
  public Object clone() throws CloneNotSupportedException {
    return super.clone();

  }

	@Override
	public String toString() {
		return "Task [jobid=" + jobid + ", stageid=" + stageid + ", taskid=" + taskid + "]";
	}

	@Override
	public int hashCode() {
		return Objects.hash(jobid, stageid, taskid);
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
		Task other = (Task) obj;
		return Objects.equals(jobid, other.jobid) && Objects.equals(stageid, other.stageid)
				&& Objects.equals(taskid, other.taskid);
	}

}
