package com.github.datasamudaya.common;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * 
 * @author arun
 *
 */
@Setter
@Getter
@AllArgsConstructor
@NoArgsConstructor
public class TaskInfoYARN {
	private String jobid;
	private boolean tokillcontainer;
	private boolean isresultavailable;
}
