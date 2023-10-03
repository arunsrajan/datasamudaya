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
package com.github.datasamudaya.mr.examples.join;


import org.apache.log4j.Logger;

import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationBuilder;
import com.github.datasamudaya.tasks.scheduler.MapReduceApplicationYarn;

public class MrJobArrivalDelayYARN implements com.github.datasamudaya.tasks.scheduler.Application {
	static String heapsize = "1024";
	static Logger log = Logger.getLogger(MrJobArrivalDelayYARN.class);

	@Override
	public void runMRJob(String[] args, JobConfiguration jobconfiguration) {
		jobconfiguration.setBatchsize("2");
		jobconfiguration.setNumofreducers("1");
		jobconfiguration.setGctype(DataSamudayaConstants.ZGC);
		jobconfiguration.setNumberofcontainers(args[3]);
		jobconfiguration.setMaxmem(args[4]);
		jobconfiguration.setIsblocksuserdefined("false");
		jobconfiguration.setExecmode(DataSamudayaConstants.EXECMODE_YARN);
		var datasamudayajob = (MapReduceApplicationYarn) MapReduceApplicationBuilder.newBuilder()
				.addMapper(new CarriersDataMapper(), args[1])
				.addMapper(new AirlineArrDelayDataMapper(), args[0])
				.addMapper(new AirlineDepDelayDataMapper(), args[0])
				.addCombiner(new CarriersDataMapper())
				.addReducer(new CarriersDataMapper())
				.setOutputfolder(args[2])
				.setJobConf(jobconfiguration)
				.build();
		var ctx = datasamudayajob.call();
		log.info(ctx);
	}
}
