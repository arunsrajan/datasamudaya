package com.github.datasamudaya.common;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * The class instance is sent to remote task executors for cleanup task actors 
 * @author arun
 *
 */
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class CleanupTaskActors implements Serializable {

	private static final long serialVersionUID = -5451274023710227938L;

	private String jobid;
	private List<String> stageids = new ArrayList<>();
	private List<String> stageidtasks = new ArrayList<>();
}
