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
package com.github.datasamudaya.stream;

import static org.junit.Assert.assertEquals;
import org.junit.Test;

import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;

public class StreamPipelineTest extends StreamPipelineTestCommon {

	@Test
	public void testGetDAG() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsurl, hdfsdirpaths1[0], pc);
		StreamPipeline<String[]> mdparr = mdp.map((val) -> val.split(DataSamudayaConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		Job job = new Job();
		job.setJm(new JobMetrics());
		mdparr.getDAG(job);
		assertEquals(1, job.getStageoutputmap().size());
		assertEquals(1, job.getTopostages().size());
	}

	@Test
	public void testFormDAGAbstractFunction() throws Exception {
		PipelineConfig pc = new PipelineConfig();
		StreamPipeline<String> mdp = StreamPipeline.newStreamHDFS(hdfsurl, hdfsdirpaths1[0], pc);
		StreamPipeline<String[]> mdparr = mdp.map((val) -> val.split(DataSamudayaConstants.COMMA));
		mdparr.finaltasks.add(mdparr.task);
		mdparr.mdsroots.add(mdp);
		mdparr.formDAGAbstractFunction(null, mdparr.mdsroots);
		assertEquals(2, mdparr.graph.vertexSet().size());
	}
}
