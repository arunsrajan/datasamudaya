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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import java.net.URI;
import java.util.Objects;
import java.util.Set;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import com.github.datasamudaya.common.utils.HadoopTestUtilities;
import com.github.datasamudaya.common.utils.Utils;

public class RemoteDataFetcherTest {

	static MiniDFSCluster hdfsLocalCluster;
	static int namenodeport = 9000;
	static int namenodehttpport = 9870;

	@BeforeClass
	public static void setUpHdfs() throws Exception {
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
		System.setProperty("HADOOP_HOME", "C:\\DEVELOPMENT\\hadoop\\hadoop-3.3.4");
		hdfsLocalCluster = HadoopTestUtilities.initHdfsCluster(9000, 9870, 2);
		Utils.initializeProperties(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_TEST_PROPERTIES);
		System.setProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFS() throws Exception {
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		String testdata = "TestData";
		ctx.put(testdata, testdata);
		String filename = "TestFile.dat";
		String job = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
		RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
		Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
		assertNotNull(keys);
		assertTrue(keys.size() == 1);
		ctx = null;
		ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
		assertNotNull(ctx);
		assertTrue(ctx.keys().size() == 1);
		assertTrue(ctx.get(testdata).contains(testdata));
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFSReadException() {
		try {
			DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
			String testdata = "TestData";
			ctx.put(testdata, testdata);
			String filename = "TestFile1.dat";
			String unavailablefilename = "TestFile2.dat";
			String job = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
			assertNotNull(keys);
			assertTrue(keys.size() == 1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, unavailablefilename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size() == 1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch (Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ex.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testWriteReadInterMediateOutputToFromDFSReadKeysException() {
		try {
			DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
			String testdata = "TestData";
			ctx.put(testdata, testdata);
			String filename = "TestFile1.dat";
			String unavailablefilename = "TestFile2.dat";
			String job = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			RemoteDataFetcher.writerIntermediatePhaseOutputToDFS(ctx, job, filename);
			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, unavailablefilename, true);
			assertNotNull(keys);
			assertTrue(keys.size() == 1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size() == 1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch (Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEREADERROR, ex.getMessage());
		}
	}

	@SuppressWarnings("unchecked")
	@Test
	public void testCreateFileMR() throws Exception {
		String filename = "TestFile1.dat";
		String job = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
		Configuration configuration = new Configuration();
		configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
		configuration.set(DataSamudayaConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + DataSamudayaConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + job;
		String filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
		// Create folders if not already created.
		Path filepathurl = new Path(filepath);
		FileSystem hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
				configuration);
		String testdata = "TestData";
		DataCruncherContext<String, String> ctx = new DataCruncherContext<>();
		ctx.put(testdata, testdata);
		RemoteDataFetcher.createFileMR(hdfs, filepathurl, ctx);

		Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
		assertNotNull(keys);
		assertTrue(keys.size() == 1);
		ctx = null;
		ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
		assertNotNull(ctx);
		assertTrue(ctx.keys().size() == 1);
		assertTrue(ctx.get(testdata).contains(testdata));
	}

	@SuppressWarnings({"unchecked", "rawtypes"})
	@Test
	public void testCreateFileMRException() {
		try {
			String filename = "TestFile1.dat";
			String job = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
			configuration.set(DataSamudayaConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL);
			FileSystem hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			String testdata = "TestData";
			DataCruncherContext ctx = new DataCruncherContext();
			ctx.put(testdata, testdata);
			RemoteDataFetcher.createFileMR(hdfs, null, ctx);

			Set<String> keys = (Set<String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, true);
			assertNotNull(keys);
			assertTrue(keys.size() == 1);
			ctx = null;
			ctx = (DataCruncherContext<String, String>) RemoteDataFetcher.readIntermediatePhaseOutputFromDFS(job, filename, false);
			assertNotNull(ctx);
			assertTrue(ctx.keys().size() == 1);
			assertTrue(ctx.get(testdata).contains(testdata));
		}
		catch (Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex.getMessage());
		}
	}

	@Test
	public void testCreateFileException() {
		try {
			String filename = "TestFile1.dat";
			String dir = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
			configuration.set(DataSamudayaConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + DataSamudayaConstants.FORWARD_SLASH
					+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + dir;
			String filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
			Path filepathurl = new Path(filepath);
			JobStage js = new JobStage();
			RemoteDataFetcher.createFile(null, filepathurl, js, new PipelineConfig());
		}
		catch (Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEWRITEERROR, ex.getMessage());
		}
	}


	@Test
	public void testdeleteFolder() throws Exception {
		String filename = "TestFiledelete1.dat";
		String dir = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
		Configuration configuration = new Configuration();
		configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS,
				DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
		configuration.set(DataSamudayaConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
		configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
		String jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + DataSamudayaConstants.FORWARD_SLASH
				+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + dir;
		String filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
		Path filepathurl = new Path(filepath);
		FileSystem hdfs = FileSystem
				.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)), configuration);
		JobStage js = new JobStage();
		PipelineConfig pc = new PipelineConfig();
		pc.setClsloader(this.getClass().getClassLoader());
		RemoteDataFetcher.createFile(hdfs, filepathurl, js, pc);
		assertTrue(hdfs.exists(filepathurl));
		RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS(dir);
		assertFalse(hdfs.exists(filepathurl));

	}

	@Test
	public void testdeleteFolderException() {
		try {
			String filename = "TestFiledelete2.dat";
			String dir = DataSamudayaConstants.JOB + DataSamudayaConstants.HYPHEN + System.currentTimeMillis();
			Configuration configuration = new Configuration();
			configuration.set(DataSamudayaConstants.HDFS_DEFAULTFS, DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL));
			configuration.set(DataSamudayaConstants.HDFS_IMPL, org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());
			configuration.set(DataSamudayaConstants.HDFS_FILE_IMPL, org.apache.hadoop.fs.LocalFileSystem.class.getName());
			String jobpath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL) + DataSamudayaConstants.FORWARD_SLASH
					+ FileSystemSupport.MDS + DataSamudayaConstants.FORWARD_SLASH + dir;
			String filepath = jobpath + DataSamudayaConstants.FORWARD_SLASH + filename;
			Path filepathurl = new Path(filepath);
			FileSystem hdfs = FileSystem.newInstance(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					configuration);
			JobStage js = new JobStage();
			PipelineConfig pc = new PipelineConfig();
			pc.setClsloader(this.getClass().getClassLoader());
			RemoteDataFetcher.createFile(hdfs, filepathurl, js, pc);
			assertTrue(hdfs.exists(filepathurl));
			RemoteDataFetcher.deleteIntermediatePhaseOutputFromDFS(jobpath);
		}
		catch (Exception ex) {
			assertEquals(RemoteDataFetcherException.INTERMEDIATEPHASEDELETEERROR, ex.getMessage());
		}
	}

	@AfterClass
	public static void closeHdfs() throws Exception {
		if(!Objects.isNull(hdfsLocalCluster)) {
			hdfsLocalCluster.shutdown(true);
		}
	}
}
