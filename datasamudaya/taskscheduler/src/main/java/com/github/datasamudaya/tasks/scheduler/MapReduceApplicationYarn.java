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

import java.io.File;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Callable;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import org.apache.curator.framework.CuratorFramework;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FileUtil;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.protocol.LocatedBlock;
import org.apache.hadoop.yarn.api.records.YarnApplicationState;
import org.apache.log4j.Logger;
import org.springframework.context.support.ClassPathXmlApplicationContext;
import org.springframework.yarn.client.CommandYarnClient;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.HDFSBlockUtils;
import com.github.datasamudaya.common.JobConfiguration;
import com.github.datasamudaya.common.JobMetrics;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaJobMetrics;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.tasks.scheduler.yarn.YarnReducer;

/**
 * Map reduce application runs in hadoop yarn.
 * @author arun
 *
 */
@SuppressWarnings("rawtypes")
public class MapReduceApplicationYarn implements Callable<List<DataCruncherContext>> {
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
	int blocksize;
	Path currentfilepath;
	int blocklocationindex;
	long redcount;
	List<BlocksLocation> bls;
	List<String> nodes;
	CuratorFramework cf;
	static Logger log = Logger.getLogger(MapReduceApplicationYarn.class);
	List<LocatedBlock> locatedBlocks;
	int executorindex;
	ExecutorService es;

	public MapReduceApplicationYarn(String jobname, JobConfiguration jobconf, List<MapperInput> mappers,
			List combiners, List reducers, String outputfolder) {
		this.jobname = jobname;
		this.jobconf = jobconf;
		this.mappers = mappers;
		this.combiners = combiners;
		this.reducers = reducers;
		this.outputfolder = outputfolder;
	}


	Map<String, ArrayBlockingQueue> containerqueue = new ConcurrentHashMap<>();
	List<Integer> ports;
	protected List<String> containers;
	protected List<String> nodessorted;

	public void getDnXref(List<BlocksLocation> bls) throws MapReduceException {
		log.debug("Entered DataSamudayaJob.getDnXref");
		var dnxrefs = bls.stream().parallel().flatMap(bl -> {
			var xrefs = new LinkedHashSet<String>();
			Iterator<Set<String>> xref = bl.getBlock()[0].getDnxref().values().iterator();
			for (; xref.hasNext(); ) {
				xrefs.addAll(xref.next());
			}
			if (bl.getBlock().length > 1 && !Objects.isNull(bl.getBlock()[1])) {
				xref = bl.getBlock()[0].getDnxref().values().iterator();
				for (; xref.hasNext(); ) {
					xrefs.addAll(xref.next());
				}
			}
			return xrefs.stream();
		}).collect(Collectors.groupingBy(key -> key.split(DataSamudayaConstants.COLON)[0],
				Collectors.mapping(xref -> xref, Collectors.toCollection(LinkedHashSet::new))));
		var dnxrefallocatecount = (Map<String, Long>) dnxrefs.keySet().stream().parallel().flatMap(key -> {
			return dnxrefs.get(key).stream();
		}).collect(Collectors.toMap(xref -> xref, xref -> 0l));

		for (var b : bls) {
			var xrefselected = b.getBlock()[0].getDnxref().keySet().stream()
					.flatMap(xrefhost -> b.getBlock()[0].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
				return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
			}).findFirst();
			var xref = xrefselected.get();
			dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
			b.getBlock()[0].setHp(xref);
			if (b.getBlock().length > 1 && !Objects.isNull(b.getBlock()[1])) {
				xrefselected = b.getBlock()[1].getDnxref().keySet().stream()
						.flatMap(xrefhost -> b.getBlock()[1].getDnxref().get(xrefhost).stream()).sorted((xref1, xref2) -> {
					return dnxrefallocatecount.get(xref1).compareTo(dnxrefallocatecount.get(xref2));
				}).findFirst();
				xref = xrefselected.get();
				dnxrefallocatecount.put(xref, dnxrefallocatecount.get(xref) + 1);
				b.getBlock()[1].setHp(xref);
			}
		}

		log.debug("Exiting DataSamudayaJob.getDnXref");
	}

	public List<DataCruncherContext> call() {
		String applicationid = DataSamudayaConstants.DATASAMUDAYAAPPLICATION + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
		try {
			var starttime = System.currentTimeMillis();
			batchsize = Integer.parseInt(jobconf.getBatchsize());
			numreducers = Integer.parseInt(jobconf.getNumofreducers());
			var configuration = new Configuration();
			blocksize = Integer.parseInt(jobconf.getBlocksize());
			var jm = new JobMetrics();
			jm.setJobstarttime(System.currentTimeMillis());
			jm.setJobid(applicationid);
			DataSamudayaJobMetrics.put(jm);
			hdfs = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			
			var combiner = new HashSet<Object>();
			var reducer = new HashSet<>();
			var mapclzchunkfile = new HashMap<String, Set<Object>>();
			hdfsdirpath = DataSamudayaConstants.EMPTY;
			for (var mapperinput : mappers) {
				try {
					if (mapclzchunkfile.get(mapperinput.inputfolderpath) == null) {
						mapclzchunkfile.put(mapperinput.inputfolderpath, new HashSet<>());
					}
					mapclzchunkfile.get(mapperinput.inputfolderpath).add(mapperinput.crunchmapper);
					hdfsdirpath += mapperinput.inputfolderpath + DataSamudayaConstants.COMMA;
				} catch (Error ex) {

				}
			}
			var hdfsdirpaths = hdfsdirpath.substring(0, hdfsdirpath.length() - 1).split(DataSamudayaConstants.COMMA);
			var mrtaskcount = 0;
			var folderfileblocksmap = new ConcurrentHashMap<>();
			var allfiles = new ArrayList<String>();
			boolean isblocksuserdefined = Boolean.parseBoolean(jobconf.getIsblocksuserdefined());
			for (var hdfsdir : hdfsdirpaths) {
				var fileStatus = hdfs.listStatus(
						new Path(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + hdfsdir));
				var paths = FileUtil.stat2Paths(fileStatus);
				blockpath.addAll(Arrays.asList(paths));
				bls = new ArrayList<>();
				bls.addAll(HDFSBlockUtils.getBlocksLocationByFixedBlockSizeAuto(hdfs, blockpath, null));
				getDnXref(bls);
				mrtaskcount += bls.size();
				folderfileblocksmap.put(hdfsdir, bls);
				allfiles.addAll(Utils.getAllFilePaths(blockpath));
				jm.setTotalfilesize(jm.getTotalfilesize() + Utils.getTotalLengthByFiles(hdfs, blockpath));
				blockpath.clear();
			}

			jm.setTotalfilesize(jm.getTotalfilesize() / DataSamudayaConstants.MB);
			jm.setFiles(allfiles);
			jm.setMode(jobconf.getExecmode());
			jm.setTotalblocks(bls.size());
			log.debug("Total MapReduce Tasks: " + mrtaskcount);
			for (var obj : combiners) {
				if (obj != null) {
					combiner.add(obj);
				}
			}
			for (var obj : reducers) {
				if (obj != null) {
					reducer.add(obj);
				}
			}
			var yarninputfolder = DataSamudayaConstants.YARNINPUTFOLDER + DataSamudayaConstants.FORWARD_SLASH + applicationid;
			var output = jobconf.getOutput();
			jobconf.setOutputfolder(outputfolder);
			jobconf.setOutput(null);
			decideContainerCountAndPhysicalMemoryByBlockSize();
			System.setProperty("jobcount", "" + mrtaskcount);
			new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
			Utils.createJar(new File(DataSamudayaConstants.YARNFOLDER), DataSamudayaConstants.LOCAL_FS_APPJRPATH, DataSamudayaConstants.YARNOUTJAR);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(mapclzchunkfile, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_MAPPER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(combiner, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_COMBINER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(reducer, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_REDUCER, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(folderfileblocksmap, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_FILEBLOCKS, jobconf);
			RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS(jobconf, yarninputfolder,
					DataSamudayaConstants.MASSIVEDATA_YARNINPUT_CONFIGURATION, jobconf);
			ClassPathXmlApplicationContext context = new ClassPathXmlApplicationContext(
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.CONTEXT_FILE_CLIENT, getClass());
			var client = (CommandYarnClient) context.getBean(DataSamudayaConstants.YARN_CLIENT);
			client.getEnvironment().put(DataSamudayaConstants.YARNDATASAMUDAYAJOBID, applicationid);
			var appid = client.submitApplication(true);
			var appreport = client.getApplicationReport(appid);
			while (appreport.getYarnApplicationState() != YarnApplicationState.FINISHED
					&& appreport.getYarnApplicationState() != YarnApplicationState.FAILED) {
				appreport = client.getApplicationReport(appid);
				Thread.sleep(1000);
			}

			log.debug("Waiting for the Reducer to complete------------");
			log.debug("Reducer completed------------------------------");
			jobconf.setOutput(output);
			jm.setJobcompletiontime(System.currentTimeMillis());
			jm.setTotaltimetaken((jm.getJobcompletiontime() - jm.getJobstarttime()) / 1000.0);
			if (!Objects.isNull(jobconf.getOutput())) {
				Utils.writeToOstream(jobconf.getOutput(),
						"Completed Job in " + (jm.getTotaltimetaken()) + " seconds");
			}
			List<YarnReducer> reducers = (List<YarnReducer>) RemoteDataFetcher
					.readYarnAppmasterServiceDataFromDFS(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL),
					yarninputfolder, DataSamudayaConstants.MASSIVEDATA_YARN_RESULT);
			List<DataCruncherContext> results = new ArrayList<>();
			for (YarnReducer red: reducers) {
				var ctxreducerpart = (Context) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(
						red.getApptask().getApplicationid(),
						(red.getApptask().getApplicationid() + red.getApptask().getTaskid()), false);
				results.add((DataCruncherContext) ctxreducerpart);
			}
			return results;
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error("Unable To Execute Job, See Cause Below:", ex);
		}
		return null;
	}


	/**
	 * Calculate the container count via number of processors and container
	 * memory
	 */
	private void decideContainerCountAndPhysicalMemoryByBlockSize() {
		long processors = Integer.parseInt(jobconf.getNumberofcontainers());
		System.setProperty("containercount", "" + (processors - 2));
		System.setProperty("containermemory", "" + Integer.parseInt(jobconf.getMaxmem()));
		System.setProperty("hd.fs", jobconf.getHdfsurl());
		System.setProperty("hd.rm", jobconf.getYarnrm());
		System.setProperty("hd.scheduler", jobconf.getYarnscheduler());
	}

}
