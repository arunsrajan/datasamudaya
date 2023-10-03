package com.github.datasamudaya.stream.examples;

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
import java.io.Serializable;
import java.net.URI;
import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.IgnitePipeline;
import com.github.datasamudaya.stream.Pipeline;

public class IgnitePipelineFile implements Serializable, Pipeline {
	private static final long serialVersionUID = 33623853570356905L;
	private Logger log = Logger.getLogger(StreamReduceIgnite.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		pipelineconfig.setIsblocksuserdefined("false");
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		testReduce(args, pipelineconfig);
	}

	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("IgnitePipelineFile.testReduce Before---------------------------------------");
		var datastream = IgnitePipeline.newStreamFILE(args[0],
				pipelineconfig);
		var mappair1 = datastream.map(dat -> dat.split(","))
				.filter(dat -> !"ArrDelay".equals(dat[14]) && !"NA".equals(dat[14]))
				.mapToPair(dat -> Tuple.tuple(dat[8], Long.parseLong(dat[14])));

		var cachedreduceByKey = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2);
		
		
		cachedreduceByKey.saveAsTextFile(URI.create("file:C:/DEVELOPMENT/datasamudayaoutput"), "/IgnitePipelineFile-" + System.currentTimeMillis());
		
		cachedreduceByKey.coalesce(1,
				(dat1, dat2) -> dat1 + dat2).saveAsTextFile(URI.create("file:C:/DEVELOPMENT/datasamudayaoutput"), "/IgnitePipelineFileCoalesce-" + System.currentTimeMillis());
				log.info("IgnitePipelineFile.testReduce After---------------------------------------");
	}
}
