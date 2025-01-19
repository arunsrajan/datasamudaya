package com.github.datasamudaya.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TaskExecutorKillResponse {
	private String user;
	private String eid;
	private String node;
	private String executor;
	private String status;
	private String message;
}
