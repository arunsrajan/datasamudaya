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

import java.io.OutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;

import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import lombok.ToString;

/**
 * 
 * @author Arun
 * Holder of job information required to execute the job
 */
@Getter
@Setter
@EqualsAndHashCode
@ToString
@NoArgsConstructor
@AllArgsConstructor
public class Job implements Serializable{
	private static final long serialVersionUID = 2778442248313879982L;
	public static enum TRIGGER {
		COUNT,COLLECT,SAVERESULTSTOFILE,FOREACH,PIGDUMP
	}
	public static enum JOBTYPE {
		PIG,NORMAL
	}
	private String id;
	private ConcurrentMap<Stage, Object> stageoutputmap;
	private ConcurrentMap<String, String> allstageshostport;
	private List<Stage> topostages = new Vector<>();
	private Long noofpartitions;
	private List<String> taskexecutors;
	private Set<String> nodes;
	private IgniteCache<Object, byte[]> igcache;
	private Ignite ignite;
	private List<Object> input = new Vector<>();
	private List<Object> output = new Vector<>();
	private Object results;
	private boolean isresultrequired;
	private JobMetrics jm;
	private PipelineConfig pipelineconfig;
	private boolean iscompleted;
	private Set vertices;
	private Set<DAGEdge> edges;
	private TRIGGER trigger;
	private String uri;
	private String savepath;
	private List<LaunchContainers> lcs;
	private AtomicInteger stageidgenerator = new AtomicInteger(1);
	private AtomicInteger taskidgenerator = new AtomicInteger(1);
	transient ConcurrentMap<String, OutputStream> resultstream;
	private JOBTYPE jobtype = JOBTYPE.NORMAL;
}
