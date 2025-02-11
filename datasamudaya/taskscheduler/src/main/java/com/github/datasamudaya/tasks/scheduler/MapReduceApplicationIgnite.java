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

import java.io.ByteArrayOutputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;

import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaIgniteClient;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.utils.FileBlocksPartitionerHDFS;
import com.github.datasamudaya.tasks.executor.Combiner;
import com.github.datasamudaya.tasks.executor.Mapper;
import com.github.datasamudaya.tasks.scheduler.ignite.IgniteMapperCombiner;
import com.github.datasamudaya.tasks.scheduler.ignite.IgniteReducer;
import com.github.datasamudaya.tasks.scheduler.ignite.MapReduceResult;
import com.github.dexecutor.core.DefaultDexecutor;
import com.github.dexecutor.core.DexecutorConfig;
import com.github.dexecutor.core.ExecutionConfig;
import com.github.dexecutor.core.task.Task;
import com.github.dexecutor.core.task.TaskProvider;

/**
 * Map reduce runs in ignite server.
 * 
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class MapReduceApplicationIgnite implements Callable<List<DataCruncherContext>> {
	String jobname;
	JobConfiguration jobconf;
	protected List<MapperInput> mappers;
	protected List<Object> combiners;
	protected List<Object> reducers;
	String outputfolder;
	int batchsize;
	int numreducers;
	public String hdfsdirpath;
	FileSystem hdfs;
	List<Path> blockpath = new ArrayList<>();
	int totalreadsize;
	byte[] read1byt = new byte[1];
	Path currentfilepath;
	int blocklocationindex;
	long redcount;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = LogManager.getLogger(MapReduceApplicationIgnite.class);
	List<LocatedBlock> locatedBlocks;
	int executorindex;
	ExecutorService es;

	public MapReduceApplicationIgnite(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List<Object> combiners, List<Object> reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}

	List<MapReduceResult> mrresults = new ArrayList<>();
	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;
	private Semaphore semaphore;
	Ignite ignite;
	IgniteCache<Object, byte[]> ignitecache;
	IgniteCache<Object, DataCruncherContext> cachemr;

	@SuppressWarnings("unchecked")
	public List<DataCruncherContext> call() {
		try (var ostream = new ByteArrayOutputStream();
				Output combostream = new Output(ostream);
				var ostreamr = new ByteArrayOutputStream();
				Output redostream = new Output(ostreamr);) {
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			hdfs = FileSystem.get(
					new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);

			var mapclzchunkfile = new HashMap<String, Set<Object>>();
			hdfsdirpath = DataSamudayaConstants.EMPTY;
			semaphore = new Semaphore(Integer.parseInt(jobconf.getBatchsize()));
			var hdfsdirpaths = new LinkedHashSet<String>();
			for (var mapperinput : mappers) {
				try {
					if (mapclzchunkfile.get(mapperinput.inputfolderpath) == null) {
						mapclzchunkfile.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclzchunkfile.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper);
					hdfsdirpaths.add(mapperinput.inputfolderpath);
				} catch (Error ex) {

				}
			}

			var jm = new JobMetrics();
			jm.setJobstarttime(System.currentTimeMillis());
			jm.setJobid(DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN
					+ System.currentTimeMillis());
			DataSamudayaJobMetrics.put(jm);
			// Starting the node
			ignite = DataSamudayaIgniteClient.instanceMR(jobconf);
			ignitecache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);
			var datasamudayamcs = new ArrayList<IgniteMapperCombiner>();
			var allfiles = new ArrayList<String>();
			var folderfileblocksmap = new ConcurrentHashMap<>();
			Kryo kryo = Utils.getKryoInstance();
			List<Combiner> combl = new ArrayList<>();
			combl.add((Combiner) combiners.get(0));
			kryo.writeClassAndObject(combostream, combl);
			combostream.flush();
			byte[] combinerbytes = ostream.toByteArray();
			for (var hdfsdir : hdfsdirpaths) {
				var fileStatus = hdfs.listStatus(new Path(
						DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + hdfsdir));
				var paths = FileUtil.stat2Paths(fileStatus);
				blockpath.addAll(Arrays.asList(paths));
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.setTotalfilesize(jm.getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, blockpath));
				bls = new ArrayList<>();
				bls.addAll(HDFSBlockUtils.getBlocksLocation(hdfs, blockpath, null));
				folderfileblocksmap.put(hdfsdir, bls);
				FileBlocksPartitionerHDFS fbp = new FileBlocksPartitionerHDFS();
				fbp.getDnXref(bls, false);
				for (var bl : bls) {
					String blkey = Utils.getBlocksLocation(bl);
					if (!ignitecache.containsKey(blkey)) {
						var databytes = HdfsBlockReader.getBlockDataMR(bl, hdfs);
						var baos = new ByteArrayOutputStream();
						var lzfos = new SnappyOutputStream(baos);
						lzfos.write(databytes);
						lzfos.flush();
						ignitecache.putIfAbsent(blkey, baos.toByteArray());
						lzfos.close();
					}
					for (var mapperinput : mapclzchunkfile.get(hdfsdir)) {
						List<Mapper> mappers = new ArrayList<>();
						mappers.add((Mapper) mapperinput);
						try (var baosm = new ByteArrayOutputStream(); var output = new Output(baosm)) {
							kryo.writeClassAndObject(output, mappers);
							output.flush();
							var datasamudayamc = new IgniteMapperCombiner(bl, baosm.toByteArray(), combinerbytes);
							datasamudayamcs.add(datasamudayamc);
						} catch (Exception ex) {

						}
					}
				}
				blockpath.clear();
			}
			jm.setTotalfilesize(jm.getTotalfilesize() / DataSamudayaConstants.MB);
			jm.setFiles(allfiles);
			jm.setMode(jobconf.getExecmode());
			jm.setTotalblocks(bls.size());
			log.debug("Total MapReduce Tasks: " + datasamudayamcs.size());
			DexecutorConfig<IgniteMapperCombiner, Boolean> configmc = new DexecutorConfig(newExecutor(),
					new TaskProviderIgniteMapperCombiner());
			DefaultDexecutor<IgniteMapperCombiner, Boolean> executormc = new DefaultDexecutor<>(configmc);

			for (var datasamudayamc : datasamudayamcs) {
				executormc.addDependency(datasamudayamc, datasamudayamc);
			}
			executormc.execute(ExecutionConfig.NON_TERMINATING);
			log.debug("Waiting for the Reducer to complete------------");
			var dccctx = new DataCruncherContext();
			cachemr = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHEMR);
			for (var mrresult : mrresults) {
				var ctx = (Context) cachemr.get(mrresult.cachekey);
				dccctx.add(ctx);
			}
			var executorser = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("MapReduceApplicationIgnite-", 0).factory());
			var ctxes = new ArrayList<Future<Context>>();
			var result = new ArrayList<DataCruncherContext>();
			kryo.writeClassAndObject(redostream, reducers.get(0));
			redostream.flush();
			var datasamudayar = new IgniteReducer(dccctx, ostreamr.toByteArray());
			ctxes.add(executorser.submit(datasamudayar));
			for (var res : ctxes) {
				result.add((DataCruncherContext) res.get());
			}

			log.debug("Reducer completed------------------------------");
			var sb = new StringBuilder();
			var partindex = 1;
			for (var ctxreducerpart : result) {
				var keysreducers = ctxreducerpart.keys();
				sb.append(DataSamudayaConstants.NEWLINE);
				sb.append("Partition ").append(partindex).append("-------------------------------------------------");
				sb.append(DataSamudayaConstants.NEWLINE);
				for (var key : keysreducers) {
					sb.append(key + DataSamudayaConstants.SINGLESPACE + ctxreducerpart.get(key));
					sb.append(DataSamudayaConstants.NEWLINE);
				}
				sb.append("-------------------------------------------------");
				sb.append(DataSamudayaConstants.NEWLINE);
				sb.append(DataSamudayaConstants.NEWLINE);
				partindex++;
			}
			var filename = DataSamudayaConstants.MAPRED + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			log.debug("Writing Results to file: " + filename);
			try (var fsdos = hdfs
					.create(new Path(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)
							+ DataSamudayaConstants.FORWARD_SLASH + this.outputfolder
							+ DataSamudayaConstants.FORWARD_SLASH + filename));) {
				fsdos.write(sb.toString().getBytes());
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
			jm.setJobcompletiontime(System.currentTimeMillis());
			jm.setTotaltimetaken((jm.getJobcompletiontime() - jm.getJobstarttime()) / 1000.0);
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(), "Completed Job in " + (jm.getTotaltimetaken()) + " seconds");
			}
			return result;
		} catch (Exception ex) {
			log.error("Unable To Execute Job, See Cause Below:", ex);
		} finally {
			if (!Objects.isNull(ignitecache)) {
				ignitecache.close();
			}
			if (!Objects.isNull(cachemr)) {
				cachemr.close();
			}
		}
		return null;
	}

	private ExecutorService newExecutor() {
		return Executors
				.newThreadPerTaskExecutor(Thread.ofVirtual().name("MapReduceApplicationIgniteExecutor-", batchsize).factory());
	}

	Semaphore resultsemaphore = new Semaphore(1);

	public class TaskProviderIgniteMapperCombiner implements TaskProvider<IgniteMapperCombiner, Boolean> {

		public Task<IgniteMapperCombiner, Boolean> provideTask(final IgniteMapperCombiner datasamudayamc) {

			return new Task<IgniteMapperCombiner, Boolean>() {

				private static final long serialVersionUID = 1L;

				public Boolean execute() {
					try {
						semaphore.acquire();
						var compute = ignite.compute(ignite.cluster().forServers());
						var mrresult = compute.affinityCall(DataSamudayaConstants.DATASAMUDAYACACHE,
								datasamudayamc.getBlocksLocation(), datasamudayamc);
						resultsemaphore.acquire();
						mrresults.add(mrresult);
						log.debug(mrresult);
						resultsemaphore.release();
						semaphore.release();
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error("TaskProviderIgnite error", e);
					}
					return true;
				}
			};
		}
	}
}
