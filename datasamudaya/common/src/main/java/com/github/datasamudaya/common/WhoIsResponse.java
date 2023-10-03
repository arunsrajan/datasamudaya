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
import lombok.Getter;
import lombok.Setter;

/**
 * 
 * @author arun
 * This class is the response class to fetch information on the status of the tasks
 * during the stage execution from task executors. This class is returned 
 * between task executors.
 */
@Getter
@Setter
public class WhoIsResponse implements Serializable{
	private static final long serialVersionUID = -9121734276018568796L;

	public static enum STATUS {
		YETTOSTART,RUNNING,COMPLETED,FAILED
	}
	private String stagepartitionid;
	private STATUS status;

	public String getStagepartitionid() {
		return stagepartitionid;
	}

	public void setStagepartitionid(String stagepartitionid) {
		this.stagepartitionid = stagepartitionid;
	}

	public STATUS getStatus() {
		return status;
	}

	public void setStatus(STATUS status) {
		this.status = status;
	}

}
