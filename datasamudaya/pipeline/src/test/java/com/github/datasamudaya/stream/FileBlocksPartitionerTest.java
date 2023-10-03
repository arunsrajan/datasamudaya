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

import static java.util.Objects.isNull;
import static org.junit.Assert.assertEquals;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import javax.management.InstanceNotFoundException;
import javax.management.MBeanRegistrationException;
import javax.management.MalformedObjectNameException;
import org.apache.ignite.Ignite;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.IgnitePipeline;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.DataSamudayaIgniteServer;

public class FileBlocksPartitionerTest extends StreamPipelineTestCommon {
	static Ignite server;

	@BeforeClass
	public static void launchNodes() throws Exception {
		if (isNull(server)) {
			Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, "datasamudayatest.properties");
			// Starting the node
			server = DataSamudayaIgniteServer.instance();
		}
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testgetJobStageBlocks() throws PipelineException, MalformedObjectNameException, MBeanRegistrationException, InstanceNotFoundException {
		Job job = new Job();
		job.setJm(new JobMetrics());
		PipelineConfig pc = new PipelineConfig();
		IgnitePipeline mdpi = IgnitePipeline
				.newStreamFILE(
						System.getProperty("user.dir") + DataSamudayaConstants.FORWARD_SLASH + "src/test/resources/ignite", pc)
				.map(val -> val.split(DataSamudayaConstants.COMMA));
		((IgnitePipeline) mdpi.root).mdsroots.add(mdpi.root);
		((IgnitePipeline) mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((IgnitePipeline) mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.getStageoutputmap()
				.get(job.getStageoutputmap().keySet().iterator().next());
		assertEquals(1, bls.size());
		assertEquals(2, bls.get(0).getBlock().length);
		assertEquals(4270834, bls.get(0).getBlock()[0].getBlockend());
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testgetJobStageBlocks32MBBlockSize() throws PipelineException, MalformedObjectNameException, MBeanRegistrationException, InstanceNotFoundException {
		Job job = new Job();
		job.setJm(new JobMetrics());
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("32");
		IgnitePipeline mdpi = IgnitePipeline.newStreamFILE("C:\\DEVELOPMENT\\dataset\\airline\\1987", pc)
				.map(val -> val.split(DataSamudayaConstants.COMMA));
		((IgnitePipeline) mdpi.root).mdsroots.add(mdpi.root);
		((IgnitePipeline) mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((IgnitePipeline) mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.getStageoutputmap()
				.get(job.getStageoutputmap().keySet().iterator().next());
		assertEquals(4, bls.size());
		var sum = 0;
		for (int index = 0; index < bls.size(); index++) {
			BlocksLocation bl = bls.get(index);
			sum += bl.getBlock()[0].getBlockend() - bl.getBlock()[0].getBlockstart();
		}
		assertEquals(127162942, sum);
	}

	@SuppressWarnings({"rawtypes", "unchecked"})
	@Test
	public void testgetJobStageBlocks64MBBlockSize() throws PipelineException, MalformedObjectNameException, MBeanRegistrationException, InstanceNotFoundException {
		Job job = new Job();
		job.setJm(new JobMetrics());
		PipelineConfig pc = new PipelineConfig();
		pc.setBlocksize("64");
		IgnitePipeline mdpi = IgnitePipeline.newStreamFILE("C:\\DEVELOPMENT\\dataset\\airline\\1989", pc)
				.map(val -> val.split(DataSamudayaConstants.COMMA));
		((IgnitePipeline) mdpi.root).mdsroots.add(mdpi.root);
		((IgnitePipeline) mdpi.root).finaltasks = new HashSet<>(Arrays.asList(mdpi.root.finaltask));
		((IgnitePipeline) mdpi.root).getDAG(job);
		List<BlocksLocation> bls = (List<BlocksLocation>) job.getStageoutputmap()
				.get(job.getStageoutputmap().keySet().iterator().next());
		assertEquals(8, bls.size());
		var sum = 0;
		for (int index = 0; index < bls.size(); index++) {
			BlocksLocation bl = bls.get(index);
			sum += bl.getBlock()[0].getBlockend() - bl.getBlock()[0].getBlockstart();
		}
		assertEquals(486518821, sum);
	}

}
