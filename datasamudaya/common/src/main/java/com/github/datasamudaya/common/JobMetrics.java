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
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

import org.jgrapht.Graph;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * The class holds metrics of job execution
 * @author arun
 *
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class JobMetrics implements Serializable {
	private static final long serialVersionUID = 2885388424297557849L;
	private String jobname;
	private String jobid;
	private List<String> files;
	private String mode;
	private double totalfilesize;
	private List<String> stages;
	private List<String> containerresources;
	private Map<String, Double> containersallocated;
	private Set<String> nodes;
	private long totalblocks;
	private long jobstarttime;
	private long jobcompletiontime;
	private double totaltimetaken;
	private Map<String, List<Task>> taskexcutortasks = new ConcurrentHashMap<>();
	private Graph stageGraphs;
	private Graph taskGraphs;
	private String sqlpigquery;
}
