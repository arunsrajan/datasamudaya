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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Objects;
import java.util.concurrent.ConcurrentMap;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.log4j.Logger;
import org.ehcache.Cache;

import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.stream.PipelineException;

/**
 * 
 * @author Arun
 * Task executors thread for standalone task executors daemon.  
 */
@SuppressWarnings("rawtypes")
public class StreamPipelineTaskExecutorLocal extends StreamPipelineTaskExecutor  {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorLocal.class);
	protected ConcurrentMap<String, OutputStream> resultstream;
	public double timetaken = 0.0;

	public StreamPipelineTaskExecutorLocal(JobStage jobstage,
			ConcurrentMap<String, OutputStream> resultstream, Cache cache) {
		super(jobstage, cache);
		this.resultstream = resultstream;
	}

	/**
	 * This method returns the output stream from rdf object.
	 * @param rdf
	 * @return output stream
	 * @throws Exception
	 */
	public OutputStream getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + DataSamudayaConstants.HYPHEN
				+ rdf.getStageid() + DataSamudayaConstants.HYPHEN + rdf.getTaskid();
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		if (Objects.isNull(os)) {
			log.info("Inadequate event stream for the trail: " + path);
			return os;
		}
		else if (os instanceof ByteBufferOutputStream baos) {
			return os;
		}
		else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
	}

	/**
	 * Obtains the path using jobid, stageid and taskid.
	 * @return jobid-stageid-taskid
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN
				+ task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}


	/**
	 * Creates output stream from task object and buffersize.
	 * @param task
	 * @param buffersize
	 * @return output stream
	 * @throws PipelineException 
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task, int buffersize) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutorLocal.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirect.get(buffersize));
			resultstream.put(path, os);
			log.debug("Exiting StreamPipelineTaskExecutorLocal.createIntermediateDataToFS");
			return os;
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
		}
	}

	

	/**
	 * Creates the input stream from task object.
	 * @param task
	 * @return input stream
	 * @throws Exception 
	 */
	@Override
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorLocal.getIntermediateInputStreamFS");
		var path = getIntermediateDataFSFilePath(task);
		log.debug("Exiting StreamPipelineTaskExecutorLocal.getIntermediateInputStreamFS");
		OutputStream os = resultstream.get(path);
		if(Objects.isNull(os)) {
			throw new NullPointerException("Unable to get Result Stream for path: " + path);
		}else if(os instanceof ByteBufferOutputStream baos) {
			return new ByteBufferInputStream(baos.get().duplicate());
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
		
	}


	/**
	 * This method call computes the tasks from stages and return 
	 * whether the tasks are computed successfully.
	 */
	@Override
	public Boolean call() {
		starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.call");
		var stageTasks = getStagesTask();
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		var configuration = new Configuration();
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			log.debug("Submitted Stage " + stageTasks);
			log.debug("Running Stage " + stageTasks);
			if (task.input != null && task.parentremotedatafetch != null) {
				if(task.parentremotedatafetch!=null && task.parentremotedatafetch[0]!=null) {
					var numinputs = task.parentremotedatafetch.length;
					for (var inputindex = 0;inputindex < numinputs;inputindex++) {
						var input = task.parentremotedatafetch[inputindex];
						if (input != null) {
							var rdf = (RemoteDataFetch) input;
							var os = getIntermediateInputStreamRDF(rdf);
							if (os != null) {
								ByteBufferOutputStream bbos = (ByteBufferOutputStream) os;
								ByteBuffer buffer = bbos.get();
								task.input[inputindex] = new ByteBufferInputStream(buffer.duplicate());
							} else {
								RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
								task.input[inputindex] = new ByteArrayInputStream(rdf.getData());
							}
						}
					}
				} else if(task.input!=null && task.input[0]!=null) {
					var numinputs = task.input.length;
					for (var inputindex = 0;inputindex < numinputs;inputindex++) {
						var input = task.input[inputindex];
						if (input != null && input instanceof Task taskinput) {
							var is = getIntermediateInputStreamFS(taskinput);
							if (is != null) {
								task.input[inputindex] = is;
							}
						}
					}
				}				
			} 
			timetaken = computeTasks(task, hdfs);
			log.debug("Completed Stage " + stageTasks);
			completed=true;
		} catch (Throwable ex) {
			completed = false;
			log.error("Failed Stage: " + task.stageid, ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				endtime = System.currentTimeMillis();
				task.taskstatus = TaskStatus.FAILED;
				task.tasktype = TaskType.EXECUTEUSERTASK;
				task.stagefailuremessage = new String(baos.toByteArray());
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		}
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.call");
		return completed;
	}

}
