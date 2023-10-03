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
package com.github.datasamudaya.common;

import java.io.Serializable;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 * @author Arun
 * The Holder of job with stage information and also the job statuses
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class JobStage implements Serializable,Cloneable {
	private static final long serialVersionUID = 7292002084722232039L;
	private String jobid;
	private String stageid;
	private Stage stage;

	@Override
	public int hashCode() {
		final int prime = 31;
		int result = 1;
		result = prime * result + ((jobid == null) ? 0 : jobid.hashCode());
		result = prime * result + ((stageid == null) ? 0 : stageid.hashCode());
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
		JobStage other = (JobStage) obj;
		if (jobid == null) {
			if (other.jobid != null) {
				return false;
			}
		} else if (!jobid.equals(other.jobid)) {
			return false;
		}
		if (stageid == null) {
			if (other.stageid != null) {
				return false;
			}
		} else if (!stageid.equals(other.stageid)) {
			return false;
		}
		return true;
	}


	@Override
	public String toString() {
		return "JobStage [jobid=" + jobid + ", stageid=" + stageid + "]";
	}


	@Override
	public Object clone() throws CloneNotSupportedException {
		return super.clone();
	}
}
