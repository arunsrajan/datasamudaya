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
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author Arun
 * The Heartbeat response between task scheduler and task executor of map reduce framework.
 */
@EqualsAndHashCode
@ToString
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ApplicationTask implements Serializable {

	private static final long serialVersionUID = -3860457009382190198L;

	//This enum declaration are the statuses of the Map Reduce Framework Executor and Task Scheduler Heart Beat
	public enum TaskStatus {
		YETTOSUBMIT,SUBMITTED,RUNNING,COMPLETED,FAILED
	}

	//This enum declaration are the task type for Map Reduce Framework.
	public enum TaskType {
		REDUCER,MAPPERCOMBINER
	}

	private String applicationid;
	private String stageid;
	private String taskid;
	private TaskStatus taskstatus;
	private TaskType tasktype;
	private String hp;
	private String apperrormessage;

}
