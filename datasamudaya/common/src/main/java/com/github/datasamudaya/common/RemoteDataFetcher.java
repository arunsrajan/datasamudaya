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
package com.github.datasamudaya.common;

import java.io.BufferedInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.URI;
import java.util.LinkedHashSet;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocalFileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.log4j.Logger;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.utils.Utils;

import static java.util.Objects.*;

/**
 * 
 * @author Arun
 * Utility to get the intermediate 
 * and the final stage output from the HDFS and local file system
 */
public class RemoteDataFetcher {
	private RemoteDataFetcher() {
	}
	private static final Logger log = Logger.getLogger(RemoteDataFetcher.class);

	/**
	 * Write the intermediate and final stage output to HDFS.
	 * @param serobj
	 * @param jobid
	 * @param filename
	 * @throws Throwable
	 */
	@SuppressWarnings("rawtypes")
	public static void writerIntermediatePhaseOutputToDFS(Context serobj,
			String jobid, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.writerIntermediatePhaseOutputToDFS");
		var configuration = new Configuration();
		configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT));
		configuration.set(DataSamudayaConstants.HDFS_IMPL, DistributedFileSystem.class.getName());
		configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, LocalFileSystem.class.getName());
		var jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + jobid;
		var filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
		var jobpathurl = new Path(jobpath);
		// Create folders if not already created.
		var filepathurl = new Path(filepath);
		try (var hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)),
				configuration);) {
			if (!hdfs.exists(jobpathurl)) {
				hdfs.mkdirs(jobpathurl);
			}
			if (hdfs.exists(filepathurl)) {
				hdfs.delete(filepathurl, false);
			}
			createFileMR(hdfs, filepathurl, serobj);

		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.writerIntermediatePhaseOutputToDFS");
	}

	/**
	 * This method creates file given the path in HDFS for the MRJob API
	 * @param hdfs
	 * @param filepathurl
	 * @param serobj
	 * @throws RemoteDataFetcherException
	 */
	@SuppressWarnings({"rawtypes", "unchecked"})
	protected static void createFileMR(FileSystem hdfs, Path filepathurl, Context serobj) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.createFileMR");

		try (var fsdos = hdfs.create(filepathurl); 
				var output = new Output(fsdos);) {

			Utils.getKryo().writeClassAndObject(output, new LinkedHashSet<>(serobj.keys()));
			Utils.getKryo().writeClassAndObject(output, serobj);
			output.flush();
		} catch (Exception ex) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
		}
		log.debug("Exiting RemoteDataFetcher.createFileMR");
	}


	/**
	 * This method creates file given the path in HDFS for the pipeline API
	 * @param hdfs
	 * @param filepathurl
	 * @param serobj
	 * @throws RemoteDataFetcherException
	 */
	protected static void createFile(FileSystem hdfs, Path filepathurl, Object serobj, Object config) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.createFile");
		try (var fsdos = hdfs.create(filepathurl); 
				var output = new Output(fsdos);) {
			Kryo kryo = Utils.getKryo();
			ClassLoader clsloader = null;
			if (config instanceof PipelineConfig pc) {
				if (nonNull(pc.getClsloader())) {
					kryo.setClassLoader(pc.getClsloader());
				}
				clsloader = pc.getClsloader();
				pc.setClsloader(null);
			}
			kryo.writeClassAndObject(output, serobj);
			if (config instanceof PipelineConfig pc) {
				pc.setClsloader(clsloader);
			}
			output.flush();
		} catch (Exception ex) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex);
		}
		log.debug("Exiting RemoteDataFetcher.createFile");
	}

	/**
	 * Write intermediate data like graph, jobstage maps and topological order
	 * information 
	 * in Task scheduler to HDFS
	 * @param serobj
	 * @param dirtowrite
	 * @param filename
	 * @throws Throwable
	 */
	public static void writerYarnAppmasterServiceDataToDFS(Object serobj,
			String dirtowrite, String filename, Object config) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS");
		var configuration = new Configuration();
		configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT));
		configuration.set(DataSamudayaConstants.HDFS_IMPL, DistributedFileSystem.class.getName());
		configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, LocalFileSystem.class.getName());

		var jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + dirtowrite;
		var filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
		var jobpathurl = new Path(jobpath);
		var filepathurl = new Path(filepath);
		try (var hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), configuration);) {
			if (!hdfs.exists(jobpathurl)) {
				hdfs.mkdirs(jobpathurl);
			}
			createFile(hdfs, filepathurl, serobj, config);

		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.writerYarnAppmasterServiceDataToDFS");
	}

	/**
	 * Reads intermediate data like graph, jobstage maps and topological order
	 * information 
	 * in Yarn App master from HDFS
	 * @param dirtoread
	 * @param filename
	 * @return
	 * @throws Throwable
	 */
	public static Object readYarnAppmasterServiceDataFromDFS(String namenodeurl,
			String dirtoread, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS");
		var configuration = new Configuration();
		var path = namenodeurl + DataSamudayaConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + dirtoread + DataSamudayaConstants.FORWARD_SLASH
				+ filename;
		try (var hdfs = FileSystem.newInstance(new URI(namenodeurl),
				configuration);
				var fs = (DistributedFileSystem) hdfs;
				var dis = fs.getClient().open(new Path(path).toUri().getPath());
				var in = new Input(dis);) {
			log.debug("Exiting RemoteDataFetcher.readYarnAppmasterServiceDataFromDFS");
			return Utils.getKryo().readClassAndObject(in);
		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}

	}

	/**
	 * Read intermediate and final stage output from HDFS.
	 * @param jobid
	 * @param filename
	 * @return
	 * @throws Throwable
	 */
	public static Object readIntermediatePhaseOutputFromDFS(
			String jobid, String filename, boolean keys) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		var configuration = new Configuration();
		var path = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS
				+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH + filename;
		try (var hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)),
				configuration);
				var dis = hdfs.open(new Path(path));
				var in = new Input(dis);) {
			var keysobj = Utils.getKryo().readClassAndObject(in);
			if (keys) {
				return keysobj;
			}
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			return Utils.getKryo().readClassAndObject(in);
		} catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}

	}

	/**
	 * Gets the HDFS inputstream of a file using jobid and filename 
	 * @param jobid
	 * @param filename
	 * @param hdfs
	 * @return
	 * @throws Throwable
	 */
	public static InputStream readIntermediatePhaseOutputFromDFS(
			String jobid, String filename, FileSystem hdfs) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		try {
			var path = DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH + filename;
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			return hdfs.open(new Path(path));
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}
	}


	/**
	 * Gets the local file using jobid and filename 
	 * @param jobid
	 * @param filename
	 * @param hdfs
	 * @return
	 * @throws Throwable
	 */
	public static InputStream readIntermediatePhaseOutputFromFS(
			String jobid, String filename) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
		try {
			var path = DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH + filename;
			File file = new File(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR) + path);
			log.debug("Exiting RemoteDataFetcher.readIntermediatePhaseOutputFromDFS");
			if (file.isFile() && file.exists()) {
				return new BufferedInputStream(new FileInputStream(file));
			}
			return null;
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ioe);
		}
	}


	/**
	 * Delete all the stages outputs of a job
	 * @param jobid
	 * @throws Throwable
	 */
	public static void deleteIntermediatePhaseOutputFromDFS(
			String jobid) throws RemoteDataFetcherException {
		log.debug("Entered RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS");
		var configuration = new Configuration();
		try (var hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT)), configuration)) {
			hdfs.delete(new Path(DataSamudayaConstants.FORWARD_SLASH + FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + jobid), true);
		}
		catch (Exception ioe) {
			log.error(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR, ioe);
			throw new RemoteDataFetcherException(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR, ioe);
		}
		log.debug("Exiting RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS");
	}

	/**
	 * This method returns the intermediate data remote by passing the
	 * RemoteDataFetch object.
	 * @param rdf
	 * @throws Exception
	 */
	public static void remoteInMemoryDataFetch(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered RemoteDataFetcher.remoteInMemoryDataFetch");
		log.info("Remote data recover with hp: " + rdf.getHp());
		var rdfwithdata = (RemoteDataFetch) Utils.getResultObjectByInput(rdf.getHp(), rdf, rdf.getTejobid());
		rdf.setData(rdfwithdata.getData());
		log.debug("Exiting RemoteDataFetcher.remoteInMemoryDataFetch");
	}


}
