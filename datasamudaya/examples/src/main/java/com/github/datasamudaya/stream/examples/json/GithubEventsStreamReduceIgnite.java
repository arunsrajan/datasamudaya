package com.github.datasamudaya.stream.examples.json;

import java.io.Serializable;
import java.net.URI;

import org.apache.log4j.Logger;
import org.jooq.lambda.tuple.Tuple;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.stream.StreamPipeline;
import com.github.datasamudaya.stream.Pipeline;

public class GithubEventsStreamReduceIgnite implements Serializable, Pipeline {
	private static final long serialVersionUID = -7163128367640941539L;
	private final Logger log = Logger.getLogger(GithubEventsStreamReduceIgnite.class);

	public void runPipeline(String[] args, PipelineConfig pipelineconfig) throws Exception {
		
		pipelineconfig.setLocal("false");
		pipelineconfig.setMesos("false");
		pipelineconfig.setYarn("false");
		pipelineconfig.setJgroups("false");
		pipelineconfig.setMode(DataSamudayaConstants.MODE_DEFAULT);
		testReduce(args, pipelineconfig);
	}

	public void testReduce(String[] args, PipelineConfig pipelineconfig) throws Exception {
		log.info("GithubEventsStreamReduceIgnite.testReduce Before---------------------------------------");
		var datastream = StreamPipeline.newJsonStreamHDFS(args[0], args[1], pipelineconfig);
		var mappair1 = datastream
				.mapToPair(dat -> Tuple.tuple(dat.get("type"), 1l));

		var githubevents = mappair1.reduceByKey((dat1, dat2) -> dat1 + dat2).coalesce(1,
				(dat1, dat2) -> dat1 + dat2);


		githubevents.saveAsTextFile(new URI(args[0]), args[2] + "/githubevents-" + System.currentTimeMillis());
		log.info("GithubEventsStreamReduceIgnite.testReduce After---------------------------------------");
	}
}
