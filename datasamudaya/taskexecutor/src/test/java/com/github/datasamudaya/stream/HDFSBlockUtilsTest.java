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

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.junit.Test;

import com.github.datasamudaya.common.Block;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.NetworkUtil;

public class HDFSBlockUtilsTest extends StreamPipelineBaseTestCommon {
	static Logger log = Logger.getLogger(HDFSBlockUtilsTest.class);

	public static long TOTAL = 486518821l;
	public static long TOTAL_1987_1989 = 613681763l;
	public String hdfsurl = "hdfs://127.0.0.1:9000";
	String[] hdfsdirpaths = {"/airline1989"};
	String[] hdfsdir_1989_1987 = {"/airline1989", "/1987"};
	String host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty("taskexecutor.host"));
	int port = Integer.parseInt(DataSamudayaProperties.get().getProperty("node.port"));

	@Test
	public void testBlocksDefinedBlocksize128MB() throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsurl), conf);

		List<Path> blockpath = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths) {
			FileStatus[] fileStatus = hdfs.listStatus(
					new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			blockpath.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocation(hdfs, blockpath, null);
		long totalbytes = 0;
		for (BlocksLocation bl :bls) {
			int sum = 0;
			log.info(bl);
			for (Block b :bl.getBlock()) {
				if (!Objects.isNull(b)) {
					sum += b.getBlockend() - b.getBlockstart();
				}
			}
			totalbytes += sum;
		}
		assertEquals(TOTAL, totalbytes);
		bls = HDFSBlockUtils.getBlocksLocation(hdfs, blockpath, null);
		totalbytes = 0;
		for (BlocksLocation bl :bls) {
			log.info(bl);
			for (Block b :bl.getBlock()) {
				if (!Objects.isNull(b)) {
					totalbytes += b.getBlockend() - b.getBlockstart();
				}
			}
		}
		assertEquals(TOTAL, totalbytes);
		hdfs.close();
	}

	@Test
	public void testBlocks1987_1989_UserDefinedBlock_128MB() throws Exception {
		Configuration conf = new Configuration();
		FileSystem hdfs = FileSystem.newInstance(new URI(hdfsurl), conf);

		List<Path> blockpath = new ArrayList<>();
		for (String hdfsdir : hdfsdir_1989_1987) {
			FileStatus[] fileStatus = hdfs.listStatus(
					new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			blockpath.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocation(hdfs, blockpath, null);
		long totalbytes = 0;
		for (BlocksLocation bl :bls) {
			log.info(bl);
			for (Block b :bl.getBlock()) {
				if (!Objects.isNull(b)) {
					totalbytes += b.getBlockend() - b.getBlockstart();
				}
			}
		}
		assertEquals(TOTAL_1987_1989, totalbytes);
		hdfs.close();
	}

}
