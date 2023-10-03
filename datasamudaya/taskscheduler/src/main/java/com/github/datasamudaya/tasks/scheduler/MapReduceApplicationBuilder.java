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
package com.github.datasamudaya.tasks.scheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;

import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobConfigurationBuilder;
import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * Builder class for map reduce application.
 * @author arun
 *
 */
public class MapReduceApplicationBuilder {
	String jobname;
	JobConfiguration jobconf;
	List<MapperInput> mappers = new ArrayList<>();
	List<Object> combiners = new ArrayList<>();
	List<Object> reducers = new ArrayList<>();
	String outputfolder;

	private MapReduceApplicationBuilder() {

	}

	public static MapReduceApplicationBuilder newBuilder() {
		return new MapReduceApplicationBuilder();
	}

	public MapReduceApplicationBuilder addMapper(Object crunchmapper, String inputfolderpath) {
		mappers.add(new MapperInput(crunchmapper, inputfolderpath));
		return this;
	}

	public MapReduceApplicationBuilder addCombiner(Object crunchcombiner) {
		combiners.add(crunchcombiner);
		return this;
	}

	public MapReduceApplicationBuilder addReducer(Object crunchreducer) {
		reducers.add(crunchreducer);
		return this;
	}

	public MapReduceApplicationBuilder setJobConf(JobConfiguration jobconf) {
		this.jobconf = jobconf;
		return this;
	}

	public MapReduceApplicationBuilder setOutputfolder(String outputfolder) {
		this.outputfolder = outputfolder;
		return this;
	}

	@SuppressWarnings("rawtypes")
	public Callable<List<DataCruncherContext>> build() {
		if (jobconf == null) {
			jobconf = JobConfigurationBuilder.newBuilder().build();
		}
		if (jobconf.getExecmode().equals(DataSamudayaConstants.EXECMODE_IGNITE)) {
			return new MapReduceApplicationIgnite(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		else if (jobconf.getExecmode().equals(DataSamudayaConstants.EXECMODE_YARN)) {
			return new MapReduceApplicationYarn(jobname, jobconf, mappers, combiners, reducers, outputfolder);
		}
		return new MapReduceApplication(jobname, jobconf, mappers, combiners, reducers, outputfolder);

	}
}
