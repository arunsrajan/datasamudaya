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
package com.github.datasamudaya.stream.executors;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.IntSummaryStatistics;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToIntFunction;

import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.junit.BeforeClass;
import org.junit.Test;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Stage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.MapToPairFunction;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.PredicateSerializable;
import com.github.datasamudaya.common.functions.ReduceByKeyFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.DataSamudayaIgniteServer;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.CsvOptions;
import com.github.datasamudaya.stream.Json;
import com.github.datasamudaya.stream.StreamPipelineTestCommon;
import com.github.datasamudaya.stream.utils.FileBlocksPartitionerHDFS;

public class StreamPipelineTaskExecutorIgniteTest extends StreamPipelineTestCommon {
	ConcurrentMap<String, ByteArrayOutputStream> resultstream = new ConcurrentHashMap<>();
	static Ignite igniteserver;
	static IgniteCache<Object, byte[]> ignitecache;

	@BeforeClass
	public static void launchNodes() throws Exception {
		Utils.initializeProperties(
				DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
				"datasamudayatest.properties");

		// Starting the node
		igniteserver = DataSamudayaIgniteServer.instance();
		ignitecache = igniteserver.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
	}

	private byte[] getJobStageBytes(JobStage js) {
		Kryo kryo = Utils.getKryoInstance();
		ByteArrayOutputStream baos = new ByteArrayOutputStream();
		Output out = new Output(baos);
		kryo.writeClassAndObject(out, js);
		out.flush();
		out.close();
		return baos.toByteArray();
	}

	// CSV Test Cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersection() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls, false);
		sendDataBlockToIgniteServer(bls.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(bls.get(0), bls.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(46361, intersectiondata.size());
		is.close();
	}

	public void sendDataBlockToIgniteServer(BlocksLocation bsl) {
		try (var baos = new ByteArrayOutputStream(); var lzfos = new SnappyOutputStream(baos);) {
			var databytes = HdfsBlockReader.getBlockDataMR(bsl, hdfs);
			lzfos.write(databytes);
			lzfos.flush();
			ignitecache.put(bsl, baos.toByteArray());
		} catch (Exception e) {

		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls2.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();
		is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> intersectiondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSIntersectionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task1.jobid = js.getJobid();
		task1.stageid = js.getStageid();
		Object function = new IntersectionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		InputStream is1 = mdsti.getIntermediateInputStream(task1.jobid + task1.stageid + task1.taskid);
		List<InputStream> istreams1 = Arrays.asList(is1);
		mdsti.setTask(task2);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(bls2.get(0), bls2.get(0), hdfs);

		mdsti.setTask(task2);
		InputStream is2 = mdsti.getIntermediateInputStream(task2.jobid + task2.stageid + task2.taskid);
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskinter = new Task();
		taskinter.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		taskinter.jobid = js.getJobid();
		taskinter.stageid = js.getStageid();
		mdsti.setTask(taskinter);
		function = new IntersectionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsti.setTask(taskinter);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSIntersection(istreams1, istreams2);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStream(taskinter.jobid + taskinter.stageid + taskinter.taskid);
		List<String> intersectiondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(20, intersectiondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnion() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls, false);
		sendDataBlockToIgniteServer(bls.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(bls.get(0), bls.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(46361, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(bls1.get(0), bls2.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		Set<InputStream> istreams = new LinkedHashSet<>(Arrays.asList(is));
		task = new Task();
		task.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task.jobid = js.getJobid();
		task.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsti.setTask(task);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(istreams, Arrays.asList(bls2.get(0)), hdfs);
		is.close();

		is = mdsti.getIntermediateInputStream(task.jobid + task.stageid + task.taskid);
		List<String> uniondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamsBlockHDFSUnionDiff() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task task1 = new Task();
		task1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task1.jobid = js.getJobid();
		task1.stageid = js.getStageid();
		Object function = new UnionFunction();
		js.getStage().tasks.add(function);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), task1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths3) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths4) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));

		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(bls1.get(0), bls1.get(0), hdfs);
		Task task2 = new Task();
		task2.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		task2.jobid = js.getJobid();
		task2.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsti.setTask(task2);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(bls2.get(0), bls2.get(0), hdfs);

		InputStream is1 = mdsti.getIntermediateInputStream(task1.jobid + task1.stageid + task1.taskid);
		List<InputStream> istreams1 = Arrays.asList(is1);

		InputStream is2 = mdsti.getIntermediateInputStream(task2.jobid + task2.stageid + task2.taskid);
		List<InputStream> istreams2 = Arrays.asList(is2);

		Task taskunion = new Task();
		taskunion.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		taskunion.jobid = js.getJobid();
		taskunion.stageid = js.getStageid();
		function = new UnionFunction();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(function);
		mdsti.setTask(taskunion);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSUnion(istreams1, istreams2);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStream(taskunion.jobid + taskunion.stageid + taskunion.taskid);
		List<String> uniondata = (List<String>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(60, uniondata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMap() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<String[]> mapfilterdata = (List<String[]>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(45957, mapfilterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task calculatecounttask = new Task();
		calculatecounttask
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		calculatecounttask.jobid = js.getJobid();
		calculatecounttask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				calculatecounttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(
				calculatecounttask.jobid + calculatecounttask.stageid + calculatecounttask.taskid);
		List<Long> mapfiltercountdata = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sstask = new Task();
		sstask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sstask.jobid = js.getJobid();
		sstask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) strarr -> !"ArrDelay".equals(strarr[14]) && !"NA".equals(strarr[14]);
		ToIntFunction<String[]> toint = (ToIntFunction<String[]> & Serializable) strtoint -> Integer.parseInt(strtoint[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sstask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(sstask.jobid + sstask.stageid + sstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();

		Task maxtask = new Task();
		maxtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (ToIntFunction<String[]> & Serializable) (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Max());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		is.close();
		assertEquals(623, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		Task mintask = new Task();
		mintask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (ToIntFunction<String[]> & Serializable) (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Min());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		is.close();
		assertEquals(-89, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sumtask = new Task();
		sumtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (ToIntFunction<String[]> & Serializable) (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new Sum());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		is.close();
		assertEquals(-63278, (int) mapfiltermaxdata.get(0));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapSD() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task sdtask = new Task();
		sdtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		ToIntFunction<String[]> toint = (ToIntFunction<String[]> & Serializable) (String str[]) -> Integer.parseInt(str[14]);
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(toint);
		js.getStage().tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Double> mapfiltermaxdata = (List<Double>) Utils.getKryo().readClassAndObject(new Input(is));
		is.close();
		assertEquals(1, mapfiltermaxdata.size());
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task calcultecounttask = new Task();
		calcultecounttask
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		calcultecounttask.jobid = js.getJobid();
		calcultecounttask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(new CalculateCount());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				calcultecounttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(
				calcultecounttask.jobid + calcultecounttask.stageid + calcultecounttask.taskid);
		List<Long> mapfiltercountdata = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(45957l, (long) mapfiltercountdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecord() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<CSVRecord> filterdata = (List<CSVRecord>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(45957l, (long) filterdata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSumaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task summarystaticstask = new Task();
		summarystaticstask
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		summarystaticstask.jobid = js.getJobid();
		summarystaticstask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new SummaryStatistics());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				summarystaticstask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(
				summarystaticstask.jobid + summarystaticstask.stageid + summarystaticstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task maxtask = new Task();
		maxtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Max());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(623, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task mintask = new Task();
		mintask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Min());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(-89, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sumtask = new Task();
		sumtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfilterssdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(-63278, (long) mapfilterssdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapCSVRecordStandardDeviation() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task sdtask = new Task();
		sdtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new StandardDeviation());

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Double> mapfiltersddata = (List<Double>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		Task calcultecounttask = new Task();
		calcultecounttask
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		calcultecounttask.jobid = js.getJobid();
		calcultecounttask.stageid = js.getStageid();
		mdsti.setTask(calcultecounttask);
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CalculateCount());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), calcultecounttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(
				calcultecounttask.jobid + calcultecounttask.stageid + calcultecounttask.taskid);
		List<Long> csvreccount = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(45957l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSummaryStatistics() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);
		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task summarystaticstask = new Task();
		summarystaticstask
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		summarystaticstask.jobid = js.getJobid();
		summarystaticstask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new SummaryStatistics());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(summarystaticstask);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(
				summarystaticstask.jobid + summarystaticstask.stageid + summarystaticstask.taskid);
		List<IntSummaryStatistics> mapfilterssdata = (List<IntSummaryStatistics>) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfilterssdata.size());
		assertEquals(623, (long) mapfilterssdata.get(0).getMax());
		assertEquals(-89, (long) mapfilterssdata.get(0).getMin());
		assertEquals(-63278, (long) mapfilterssdata.get(0).getSum());
		assertEquals(45957l, mapfilterssdata.get(0).getCount());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMax() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task maxtask = new Task();
		maxtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		maxtask.jobid = js.getJobid();
		maxtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Max());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), maxtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(maxtask.jobid + maxtask.stageid + maxtask.taskid);
		List<Integer> mapfiltermaxdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(623, (int) mapfiltermaxdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVMin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task mintask = new Task();
		mintask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mintask.jobid = js.getJobid();
		mintask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Min());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), mintask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(mintask.jobid + mintask.stageid + mintask.taskid);
		List<Integer> mapfiltermindata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(-89, (int) mapfiltermindata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSum() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sumtask = new Task();
		sumtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sumtask.jobid = js.getJobid();
		sumtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sumtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(sumtask.jobid + sumtask.stageid + sumtask.taskid);
		List<Integer> mapfiltersumdata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(-63278, (int) mapfiltersumdata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamBlockHDFSMapCSVSD() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		Set<InputStream> inputtocount = new LinkedHashSet<>(Arrays.asList(is));
		ToIntFunction<CSVRecord> csvint = (ToIntFunction<CSVRecord> & Serializable) (CSVRecord csvrecord) -> Integer.parseInt(csvrecord.get(14));
		Task sdtask = new Task();
		sdtask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sdtask.jobid = js.getJobid();
		sdtask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(csvint);
		js.getStage().tasks.add(new Sum());
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), sdtask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(sdtask.jobid + sdtask.stageid + sdtask.taskid);
		List<Integer> mapfiltersddata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(1, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSample() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processSamplesBlocks(100, bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<Long> csvreccount = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(100, (long) csvreccount.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task counttask = new Task();
		counttask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		counttask.jobid = js.getJobid();
		counttask.stageid = js.getStageid();
		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		Object function = new CalculateCount();
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(function);
		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), counttask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processSamplesBlocks(150, bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(counttask.jobid + counttask.stageid + counttask.taskid);
		List<Long> csvreccount = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(150l, (long) csvreccount.get(0));
		is.close();
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessStreamSample() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<InputStream> inputtocount = Arrays.asList(is);
		Task sample = new Task();
		sample.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		sample.jobid = js.getJobid();
		sample.stageid = js.getStageid();
		Function samplefn = val -> val;
		js.getStage().tasks.clear();
		js.getStage().tasks.add(samplefn);
		mdsti.setTask(sample);
		mdsti.deserializeJobStage();
		mdsti.processSamplesObjects(150, inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(sample.jobid + sample.stageid + sample.taskid);
		List<Integer> mapfiltersddata = (List<Integer>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(150, (int) mapfiltersddata.size());
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessStreamSampleCount() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		CsvOptions csvoptions = new CsvOptions(airlineheader);

		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();

		PredicateSerializable<CSVRecord> filter = (PredicateSerializable<CSVRecord>& Serializable) (CSVRecord csvrecord) -> !"ArrDelay".equals(csvrecord.get(14))
				&& !"NA".equals(csvrecord.get(14));
		js.getStage().tasks.add(csvoptions);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<InputStream> inputtocount = Arrays.asList(is);
		Task counttask = new Task();
		counttask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		counttask.jobid = js.getJobid();
		counttask.stageid = js.getStageid();
		Object count = new CalculateCount();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(count);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(counttask);
		mdsti.deserializeJobStage();
		mdsti.processSamplesObjects(150, inputtocount);
		is.close();

		is = mdsti.getIntermediateInputStream(counttask.jobid + counttask.stageid + counttask.taskid);
		List<Long> mapfiltersddata = (List<Long>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(150l, (long) mapfiltersddata.get(0));
		is.close();
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessJoin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = (MapToPairFunction<String[], Tuple2<String, Integer>> & Serializable) val -> new Tuple2<String, Integer>((String) val[8],
				(Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (ReduceByKeyFunction<Integer> & Serializable) (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);
		Task reducebykeytask2 = new Task();
		reducebykeytask2
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(reducebykeytask2);
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		InputStream is1 = mdsti.getIntermediateInputStream(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStream(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = (Consumer<String> & Serializable) val -> {
		};
		js.getStage().tasks.add(dummy);
		JoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), jointask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processJoinLZF(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStream(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(tupleresult.v1.v1, tupleresult.v2.v1);
		assertEquals(tupleresult.v1.v2, tupleresult.v2.v2);
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testProcessLeftOuterJoin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = (MapToPairFunction<String[], Tuple2<String, Integer>> & Serializable) val -> new Tuple2<String, Integer>((String) val[8],
				(Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (ReduceByKeyFunction<Integer> & Serializable) (input1, input2) -> input1 + input2;
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		InputStream is1 = mdsti.getIntermediateInputStream(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStream(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = (Consumer<String> & Serializable) val -> {
		};
		js.getStage().tasks.add(dummy);
		LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (LeftOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> & Serializable) (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), jointask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processLeftOuterJoinLZF(is1, is2, jp, false, false);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStream(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertNotNull(tupleresult.v1.v1);
		assertNotNull(tupleresult.v2.v1);
		assertEquals(-63278, (int) tupleresult.v1.v2);
		assertEquals("AQ", tupleresult.v1.v1);
		assertEquals(10, (int) tupleresult.v2.v2);
		assertEquals("AQ", tupleresult.v2.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessRightOuterJoin() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();

		Task reducebykeytask1 = new Task();
		reducebykeytask1
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = (MapToPairFunction<String[], Tuple2<String, Integer>> & Serializable) val -> new Tuple2<String, Integer>((String) val[8],
				(Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (ReduceByKeyFunction<Integer> & Serializable) (input1, input2) -> input1 + input2;

		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths2) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();

		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		InputStream is1 = mdsti.getIntermediateInputStream(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStream(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task jointask = new Task();
		jointask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		jointask.jobid = js.getJobid();
		jointask.stageid = js.getStageid();
		mdsti.setTask(jointask);
		js.getStage().tasks.clear();

		Consumer<String> dummy = (Consumer<String> & Serializable) val -> {
		};
		js.getStage().tasks.add(dummy);
		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti.deserializeJobStage();
		mdsti.processRightOuterJoinLZF(is2, is1, jp, false, false);
		is1.close();
		is2.close();

		InputStream is = mdsti.getIntermediateInputStream(jointask.jobid + jointask.stageid + jointask.taskid);
		List<Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>>> mapfiltersddata = (List) Utils.getKryo()
				.readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<Tuple2<String, Integer>, Tuple2<String, Integer>> tupleresult = mapfiltersddata.get(0);
		assertNotNull(tupleresult.v1.v1);
		assertNotNull(tupleresult.v2.v1);
		assertEquals(10, (int) tupleresult.v1.v2);
		assertEquals("AQ", tupleresult.v1.v1);
		assertEquals(-63278, (int) tupleresult.v2.v2);
		assertEquals("AQ", tupleresult.v2.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessGroupByKey() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = (MapToPairFunction<String[], Tuple2<String, Integer>> & Serializable) val -> new Tuple2<String, Integer>((String) val[8],
				(Integer) Integer.parseInt(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is1 = mdsti
				.getIntermediateInputStream(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task gbktask = new Task();
		gbktask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		gbktask.jobid = js.getJobid();
		gbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Consumer<String> dummy = (Consumer<String> & Serializable) val -> {
		};
		js.getStage().tasks.add(dummy);
		gbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), gbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processGroupByKeyTuple2();
		is1.close();

		InputStream is = mdsti.getIntermediateInputStream(gbktask.jobid + gbktask.stageid + gbktask.taskid);
		List<Tuple2<String, List<Integer>>> mapfiltersddata = (List) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<String, List<Integer>> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (int) tupleresult.v2.size());
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldLeft() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = (MapToPairFunction<String[], Tuple2<String, Long>> & Serializable) val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is1 = mdsti
				.getIntermediateInputStream(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task fbktask = new Task();
		fbktask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (ReduceByKeyFunction<Long> & Serializable) (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, true);
		js.getStage().tasks.add(fbk);
		fbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(fbktask);
		mdsti.deserializeJobStage();
		mdsti.processFoldByKeyTuple2();
		is1.close();

		InputStream is = mdsti.getIntermediateInputStream(fbktask.jobid + fbktask.stageid + fbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) Utils.getKryo().readClassAndObject(new Input(is));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessFoldRight() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = (MapToPairFunction<String[], Tuple2<String, Long>> & Serializable) val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is1 = mdsti
				.getIntermediateInputStream(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task fbktask = new Task();
		fbktask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		fbktask.jobid = js.getJobid();
		fbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		ReduceByKeyFunction<Long> redfunc = (a, b) -> a + b;
		FoldByKey fbk = new FoldByKey(0l, redfunc, false);
		js.getStage().tasks.add(fbk);
		fbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), fbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processFoldByKeyTuple2();
		is1.close();

		InputStream is = mdsti.getIntermediateInputStream(fbktask.jobid + fbktask.stageid + fbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) Utils.getKryo().readClassAndObject(new Input(is));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByKey() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = (MapToPairFunction<String[], Tuple2<String, Long>> & Serializable) val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));

		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is1 = mdsti
				.getIntermediateInputStream(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task cbktask = new Task();
		cbktask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), cbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.setTask(cbktask);
		mdsti.deserializeJobStage();
		mdsti.processCountByKeyTuple2();
		is1.close();

		InputStream is = mdsti.getIntermediateInputStream(cbktask.jobid + cbktask.stageid + cbktask.taskid);
		List<Tuple2<String, Long>> mapfiltersddata = (List) Utils.getKryo().readClassAndObject(new Input(is));

		Tuple2<String, Long> tupleresult = mapfiltersddata.get(0);
		assertEquals(45957l, (long) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCountByValue() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task mappairtask1 = new Task();
		mappairtask1.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		mappairtask1.jobid = js.getJobid();
		mappairtask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Long>> pair = (MapToPairFunction<String[], Tuple2<String, Long>> & Serializable) val -> new Tuple2<String, Long>((String) val[8],
				(Long) Long.parseLong(val[14]));
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				mappairtask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is1 = mdsti
				.getIntermediateInputStream(mappairtask1.jobid + mappairtask1.stageid + mappairtask1.taskid);
		Task cbktask = new Task();
		cbktask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		cbktask.jobid = js.getJobid();
		cbktask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(new CountByKeyFunction());
		cbktask.input = new Object[]{is1};
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), cbktask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processCountByValueTuple2();
		is1.close();

		InputStream is = mdsti.getIntermediateInputStream(cbktask.jobid + cbktask.stageid + cbktask.taskid);
		List<Tuple2<Tuple2<String, Long>, Long>> mapfiltersddata = (List) Utils.getKryo()
				.readClassAndObject(new Input(is));
		int sum = 0;
		for (Tuple2<Tuple2<String, Long>, Long> tupleresult : mapfiltersddata) {
			sum += tupleresult.v2;
			assertEquals("AQ", tupleresult.v1.v1);
		}
		assertEquals(45957l, (long) sum);

	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testProcessCoalesce() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Task reducebykeytask1 = new Task();
		reducebykeytask1
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask1.jobid = js.getJobid();
		reducebykeytask1.stageid = js.getStageid();
		MapFunction<String, String[]> map = (MapFunction<String, String[]> & Serializable) (String str) -> str.split(DataSamudayaConstants.COMMA);
		PredicateSerializable<String[]> filter = (PredicateSerializable<String[]> & Serializable) (String str[]) -> !"ArrDelay".equals(str[14]) && !"NA".equals(str[14]);
		MapToPairFunction<String[], Tuple2<String, Integer>> pair = (MapToPairFunction<String[], Tuple2<String, Integer>> & Serializable) val -> new Tuple2<String, Integer>((String) val[8],
				(Integer) Integer.parseInt(val[14]));
		ReduceByKeyFunction<Integer> redfunc = (ReduceByKeyFunction<Integer> & Serializable) (input1, input2) -> input1 + input2;

		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js),
				reducebykeytask1);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>(), filepaths2 = new ArrayList<>();
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		for (String hdfsdir : hdfsdirpaths1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths2.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		List<BlocksLocation> bls2 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths2, null);
		fbp.getDnXref(bls2, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		sendDataBlockToIgniteServer(bls2.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		Task reducebykeytask2 = new Task();
		reducebykeytask2
				.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		reducebykeytask2.jobid = js.getJobid();
		reducebykeytask2.stageid = js.getStageid();
		js.getStage().tasks.clear();
		js.getStage().tasks.add(map);
		js.getStage().tasks.add(filter);
		js.getStage().tasks.add(pair);
		js.getStage().tasks.add(redfunc);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), reducebykeytask2);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls2.get(0), hdfs);

		InputStream is1 = mdsti.getIntermediateInputStream(
				reducebykeytask1.jobid + reducebykeytask1.stageid + reducebykeytask1.taskid);
		InputStream is2 = mdsti.getIntermediateInputStream(
				reducebykeytask2.jobid + reducebykeytask2.stageid + reducebykeytask2.taskid);
		Task coalescetask = new Task();
		coalescetask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		coalescetask.jobid = js.getJobid();
		coalescetask.stageid = js.getStageid();
		js.getStage().tasks.clear();
		Coalesce<Integer> coalesce = new Coalesce();
		coalesce.setCoalescepartition(1);
		coalesce.setCoalescefunction((a, b) -> a + b);
		js.getStage().tasks.add(coalesce);
		coalescetask.input = new Object[]{is1, is2};
		RightOuterJoinPredicate<Tuple2<String, Long>, Tuple2<String, Long>> jp = (Tuple2<String, Long> tup1,
				Tuple2<String, Long> tup2) -> tup1.v1.equals(tup2.v1);
		mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), coalescetask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		mdsti.deserializeJobStage();
		mdsti.processCoalesce();
		is1.close();
		is2.close();

		InputStream is = mdsti
				.getIntermediateInputStream(coalescetask.jobid + coalescetask.stageid + coalescetask.taskid);
		List<Tuple2<String, Integer>> mapfiltersddata = (List) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(1, (long) mapfiltersddata.size());
		Tuple2<String, Integer> tupleresult = mapfiltersddata.get(0);
		assertEquals(-63278 + -63278, (int) tupleresult.v2);
		assertEquals("AQ", tupleresult.v1);

	}

	// CSV Test Cases End

	// JSON Test cases Start
	@SuppressWarnings("unchecked")
	@Test
	public void testProcessBlockHDFSMapJSON() throws Exception {
		JobStage js = new JobStage();
		js.setStage(new Stage());
		js.setJobid(DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.setStageid(DataSamudayaConstants.STAGE + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		js.getStage().id = js.getStageid();
		js.getStage().tasks = new ArrayList<>();
		Json json = new Json();
		Task filtertask = new Task();
		filtertask.setTaskid(DataSamudayaConstants.TASK + DataSamudayaConstants.HYPHEN + System.currentTimeMillis());
		filtertask.jobid = js.getJobid();
		filtertask.stageid = js.getStageid();
		PredicateSerializable<JSONObject> filter = (PredicateSerializable<JSONObject> & Serializable) jsonobj -> jsonobj != null
				&& jsonobj.get("type").equals("CreateEvent");
		js.getStage().tasks.add(json);
		js.getStage().tasks.add(filter);

		StreamPipelineTaskExecutorIgnite mdsti = new StreamPipelineTaskExecutorIgnite(getJobStageBytes(js), filtertask);
		mdsti.setHdfs(hdfs);
		mdsti.ignite = igniteserver;
		mdsti.cache = ignitecache;
		List<Path> filepaths1 = new ArrayList<>();
		for (String hdfsdir : githubevents1) {
			FileStatus[] fileStatus = hdfs.listStatus(new Path(hdfsurl + hdfsdir));
			Path[] paths = FileUtil.stat2Paths(fileStatus);
			filepaths1.addAll(Arrays.asList(paths));
		}
		List<BlocksLocation> bls1 = HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, filepaths1, null);
		FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
		fbp.getDnXref(bls1, false);
		sendDataBlockToIgniteServer(bls1.get(0));
		mdsti.deserializeJobStage();
		mdsti.processBlockHDFSMap(bls1.get(0), hdfs);

		InputStream is = mdsti.getIntermediateInputStream(filtertask.jobid + filtertask.stageid + filtertask.taskid);
		List<JSONObject> jsonfilterdata = (List<JSONObject>) Utils.getKryo().readClassAndObject(new Input(is));
		assertEquals(11l, (long) jsonfilterdata.size());
		is.close();
	}

	// JSON Test cases End
}
