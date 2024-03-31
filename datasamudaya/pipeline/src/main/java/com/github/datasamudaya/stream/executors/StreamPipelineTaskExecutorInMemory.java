package com.github.datasamudaya.stream.executors;

import static java.util.Objects.isNull;

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
import org.jgroups.util.UUID;

import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.ByteBufferPoolDirectOld;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.JobStage;
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
public sealed class StreamPipelineTaskExecutorInMemory extends StreamPipelineTaskExecutor permits StreamPipelineTaskExecutorInMemoryDisk {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorInMemory.class);
	protected ConcurrentMap<String, OutputStream> resultstream;
	public double timetaken = 0.0;

	public StreamPipelineTaskExecutorInMemory(JobStage jobstage,
			ConcurrentMap<String, OutputStream> resultstream, Cache cache) {
		super(jobstage, cache);
		this.resultstream = resultstream;
	}

	/**
	 * The path from RDF to output stream.
	 * @param rdf
	 * @return output stream
	 * @throws Exception
	 */
	public OutputStream getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + DataSamudayaConstants.HYPHEN
				+ rdf.getStageid() + DataSamudayaConstants.HYPHEN + rdf.getTaskid();
		OutputStream os = resultstream.get(path);
		log.debug("Exiting StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamFS");
		if (isNull(os)) {
			return null;
		} else if (os instanceof ByteBufferOutputStream baos) {
			return baos;
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
	}

	/**
	 * The path from RDF to output stream.
	 * @param rdf
	 * @return output stream
	 * @throws Exception
	 */
	public OutputStream getIntermediateInputStreamTask(Task task) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamTask");
		var path = task.getJobid() + DataSamudayaConstants.HYPHEN
				+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid();
		OutputStream os = resultstream.get(path);
		log.debug("Exiting StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamTask");
		if (isNull(os)) {
			return null;
		} else if (os instanceof ByteBufferOutputStream baos) {
			return baos;
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}
	}

	/**
	 * Get the HDFS file path using the job id, stage id and task id.
	 * @return jobid-stageid-taskid
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN
				+ task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}

	/**
	 * Create a new BytebufferOutputStream using task and buffer size 
	 * and return it. 
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task, int buffersize) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutorInMemory.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			log.info("ResultStream Path: " + path);
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirectOld.get(buffersize));
			resultstream.put(path, os);
			log.debug("Exiting StreamPipelineTaskExecutorInMemory.createIntermediateDataToFS");
			return os;
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
		}
	}

	/**
	 * This function takes task as input and returns input stream.
	 */
	@Override
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamFS");
		var path = getIntermediateDataFSFilePath(task);
		log.debug("Exiting StreamPipelineTaskExecutorInMemory.getIntermediateInputStreamFS");
		OutputStream os = resultstream.get(path);
		if (os instanceof ByteBufferOutputStream baos) {
			return new ByteBufferInputStream(baos.get());
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
		log.debug("Entered StreamPipelineTaskExecutorInMemory.call for task " + task);
		String stageTasks = "";
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		log.info("Acclaimed namenode URL " + hdfsfilepath);
		log.info("Result Stream " + resultstream);
		var configuration = new Configuration();
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			stageTasks = getStagesTask();
			log.info("Submitted Task " + task);
			if (task.input != null && task.parentremotedatafetch != null) {
				if (task.parentremotedatafetch != null && task.parentremotedatafetch[0] != null) {
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
				} else if (task.input != null && task.input[0] != null) {
					var numinputs = task.input.length;
					for (var inputindex = 0;inputindex < numinputs;inputindex++) {
						var input = task.input[inputindex];
						if (input != null && input instanceof Task taskinput) {
							var os = getIntermediateInputStreamTask(taskinput);
							log.info("Task Input " + taskinput.jobid + " Os:" + os);
							if (os != null) {
								ByteBufferOutputStream bbos = (ByteBufferOutputStream) os;
								ByteBuffer buffer = bbos.get();
								task.input[inputindex] = new ByteBufferInputStream(buffer.duplicate());
							}
						}
					}
				}
			}
			log.info("Functioning Task " + task);
			log.info("Task Input length" + task.input.length);
			log.info("Task Input " + task.input[0]);
			task.taskexecutionstartime = starttime;
			timetaken = computeTasks(task, hdfs);
			endtime = task.taskexecutionendtime = System.currentTimeMillis();
			task.timetakenseconds = timetaken;
			log.info("Completed Task: " + task);
			task.piguuid = UUID.randomUUID().toString();
			completed = true;
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
		} finally {
			if (!Objects.isNull(hdfs)) {
				try {
					hdfs.close();
				} catch (Exception e) {
					log.error("HDFS client close error: ", e);
				}
			}
		}
		log.debug("Exiting StreamPipelineTaskExecutorInMemory.call");
		return completed;
	}

}
