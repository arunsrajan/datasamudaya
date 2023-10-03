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

import java.io.BufferedInputStream;
import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.function.IntSupplier;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.fs.FileSystem;
import org.apache.ignite.Ignite;
import org.apache.ignite.IgniteCache;
import org.apache.ignite.lang.IgniteRunnable;
import org.apache.ignite.resources.IgniteInstanceResource;
import org.apache.log4j.Logger;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Blocks;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.CountByKeyFunction;
import com.github.datasamudaya.common.functions.CountByValueFunction;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.GroupByKeyFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.IntersectionFunction;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.PipelineCoalesceFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;
import com.github.datasamudaya.common.functions.UnionFunction;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.CsvOptions;
import com.github.datasamudaya.stream.Json;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.PipelineIntStreamCollect;
import com.github.datasamudaya.stream.PipelineUtils;
import com.github.datasamudaya.stream.utils.StreamUtils;

/**
 * This class executes tasks in ignite.
 * @author Arun
 */
@SuppressWarnings("rawtypes")
public class StreamPipelineTaskExecutorIgnite implements IgniteRunnable {

	private static final long serialVersionUID = -3824414146677196362L;
	protected JobStage jobstage;
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorIgnite.class);
	protected FileSystem hdfs;
	protected String hdfspath;
	protected boolean completed;
	Task task;
	@IgniteInstanceResource
	Ignite ignite;

	IgniteCache<Object, byte[]> cache;

	public StreamPipelineTaskExecutorIgnite(JobStage jobstage, Task task) {
		this.jobstage = jobstage;
		this.task = task;
	}

	public boolean isCompleted() {
		return completed;
	}

	public FileSystem getHdfs() {
		return hdfs;
	}

	public void setHdfs(FileSystem hdfs) {
		this.hdfs = hdfs;
	}

	public Task getTask() {
		return task;
	}

	public void setTask(Task task) {
		this.task = task;
	}
	
	public String getHdfspath() {
		return hdfspath;
	}

	public void setHdfspath(String hdfspath) {
		this.hdfspath = hdfspath;
	}

	/**
	 * Get the list of all the functions.
	 * 
	 * @return
	 */
	@SuppressWarnings({ "unchecked" })
	protected List getFunctions() {
		log.debug("Entered MassiveDataStreamTaskIgnite.getFunctions");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting MassiveDataStreamTaskIgnite.getFunctions");
		return functions;
	}

	protected String getStagesTask() {
		log.debug("Entered MassiveDataStreamTaskIgnite.getStagesTask");
		var tasks = jobstage.getStage().tasks;
		var builder = new StringBuilder();
		for (var task : tasks) {
			builder.append(PipelineUtils.getFunctions(task));
			builder.append(", ");
		}
		log.debug("Exiting MassiveDataStreamTaskIgnite.getStagesTask");
		return builder.toString();
	}

	/**
	 * Process the data using intersection function.
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	public double processBlockHDFSIntersection(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var bais1 = getIntermediateInputStreamFS(blocksfirst);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = getIntermediateInputStreamFS(blockssecond);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines().parallel();
				var streamsecond = buffer2.lines().parallel();) {
			var setsecond = (Set) streamsecond.distinct().collect(Collectors.toSet());
			// Get the result of intersection functions parallel.
			var result = (List) streamfirst.distinct().filter(setsecond::contains)
					.collect(Collectors.toCollection(Vector::new));

			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}

	}

	/**
	 * Process the data using intersection function.
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSIntersection(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next())
						);
				var bais2 = getIntermediateInputStreamFS(blockssecond.get(0));
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines().parallel();) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var setsecond = (Set) streamsecond.distinct().collect(Collectors.toSet());
			// Parallel execution of the intersection function.
			var result = (List) datafirst.parallelStream().distinct().filter(setsecond::contains)
					.collect(Collectors.toCollection(Vector::new));
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Process the data using intersection function.
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSIntersection(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next())
						);
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next())
						);

		) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var datasecond = (List) Utils.getKryo().readClassAndObject(inputsecond);
			// parallel execution of intersection function.
			var result = (List) datafirst.parallelStream().distinct().filter(datasecond::contains)
					.collect(Collectors.toCollection(Vector::new));
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSIntersection");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Intersection Task is " + timetaken + " seconds");
			log.debug("GC Status Intersection task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSINTERSECTION, ex);
		}
	}

	/**
	 * Get the HDFS file path using the task id.
	 * @param task
	 * @return taskid
	 */
	public String getIntermediateDataTaskId(Task task) {
		return task.taskid;
	}

	/**
	 * Creates and returns byte array output stream
	 * @return byte array output stream
	 * @throws Exception
	 */
	public ByteArrayOutputStream createIntermediateDataToFS() throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskIgnite.createIntermediateDataToFS");
		return new ByteArrayOutputStream();
	}

	/**
	 * Get the data stream from cache.
	 * @return input stream
	 * @throws Exception
	 */
	public InputStream getIntermediateInputStreamFS(Object task) throws Exception {
		return new SnappyInputStream(new ByteArrayInputStream(cache.get(task)));
	}

	/**
	 * Open the already existing input stream using the task object as key 
	 * from ehcache.
	 * @return input stream
	 * @throws Exception
	 */
	public InputStream getIntermediateInputStream(Object task) throws Exception {
		return new ByteArrayInputStream(cache.get(task));
	}

	/**
	 * Perform the union operation
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var bais1 = getIntermediateInputStreamFS(blocksfirst);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = getIntermediateInputStreamFS(blockssecond);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines().parallel();
				var streamsecond = buffer2.lines().parallel();) {
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(streamfirst, streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(streamfirst, streamsecond).distinct()
						.collect(Collectors.toCollection(Vector::new));
			}

			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform the union operation
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next())
						);
				var bais2 = getIntermediateInputStreamFS(blockssecond.get(0));
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines().parallel();) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(java.util.stream.Stream.concat(datafirst.parallelStream(), streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(datafirst.parallelStream(), streamsecond).distinct()
						.collect(Collectors.toCollection(Vector::new));
			}
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform the union operation
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({ "unchecked" })
	public double processBlockHDFSUnion(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSUnion");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = new Input(new BufferedInputStream(fsstreamfirst.iterator().next())
						);
				var inputsecond = new Input(new BufferedInputStream(fsstreamsecond.iterator().next())
						);) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var datasecond = (List) Utils.getKryo().readClassAndObject(inputsecond);
			List result;
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			if (terminalCount) {
				result = new ArrayList<>();
				result.add(java.util.stream.Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().count());
			} else {
				// parallel stream union operation result
				result = (List) java.util.stream.Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().collect(Collectors.toCollection(Vector::new));
			}

			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();

			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSUnion");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Union Task is " + timetaken + " seconds");
			log.debug("GC Status Union task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSUNION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSUNION, ex);
		}
	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * @param blockslocation
	 * @param hdfs
	 * @return timetakes in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(BlocksLocation blockslocation, FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSMap");
		var function = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var bais = getIntermediateInputStreamFS(blockslocation);
				var buffer = new BufferedReader(new InputStreamReader(bais));) {

			List datastream = null;
			var tasks = jobstage.getStage().tasks;
			Stream intermediatestreamobject;
			if (jobstage.getStage().tasks.get(0) instanceof Json) {
				intermediatestreamobject = buffer.lines().parallel();
				intermediatestreamobject = intermediatestreamobject.map(line -> {
					try {
						return new JSONParser().parse((String) line);
					} catch (ParseException e) {
						return null;
					}
				});
			} else {
				if (jobstage.getStage().tasks.get(0) instanceof CsvOptions csvoptions) {

					var csvformat = CSVFormat.DEFAULT;
					csvformat = csvformat.withHeader(csvoptions.getHeader());

					try (var records = csvformat.parse(buffer);) {
						datastream = records.getRecords();
						intermediatestreamobject = datastream.parallelStream();
					} catch (IOException ioe) {
						log.error(PipelineConstants.FILEIOERROR, ioe);
						throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
					} catch (Exception ex) {
						log.error(PipelineConstants.PROCESSHDFSERROR, ex);
						throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
					}
				} else {
					intermediatestreamobject = buffer.lines().parallel();
				}

			}
			intermediatestreamobject.onClose(() -> {
				log.debug("Stream closed");
			});

			try (var streammap = (BaseStream) StreamUtils.getFunctionsToStream(getFunctions(),
					intermediatestreamobject);) {
				List out;

				if (function instanceof CalculateCount) {
					out = new Vector<>();
					if (streammap instanceof IntStream intstr) {
						out.add(intstr.count());
					} else {
						out.add(((Stream) streammap).count());
					}
				} else if (function instanceof PipelineIntStreamCollect piplineistream) {
					out = new Vector<>();
					out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else if (function instanceof SummaryStatistics) {
					out = new Vector<>();
					out.add(((IntStream) streammap).summaryStatistics());

				} else if (function instanceof Max) {
					out = new Vector<>();
					out.add(((IntStream) streammap).max().getAsInt());

				} else if (function instanceof Min) {
					out = new Vector<>();
					out.add(((IntStream) streammap).min().getAsInt());

				} else if (function instanceof Sum) {
					out = new Vector<>();
					out.add(((IntStream) streammap).sum());

				} else if (function instanceof StandardDeviation) {
					out = new Vector<>();
					var streamtmp = ((java.util.stream.IntStream) streammap).boxed().collect(Collectors.toList());
					var mean = streamtmp.stream().mapToInt(Integer.class::cast).average().getAsDouble();
					var variance = streamtmp.stream().mapToInt(Integer.class::cast)
							.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
					var standardDeviation = Math.sqrt(variance);
					out.add(standardDeviation);

				} else {
					out = (List) ((Stream) streammap).collect(Collectors.toCollection(Vector::new));
				}
				Utils.getKryo().writeClassAndObject(output, out);
				output.flush();

				cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
				log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSMap");
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
				log.debug("GC Status Map task:" + Utils.getGCStats());
				return timetaken;
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSHDFSERROR, ex);
				throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
			}
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		}

	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * @param fsstreamfirst
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(Set<InputStream> fsstreamfirst) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processBlockHDFSMap");
		var function = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);) {

			var functions = getFunctions();

			List out = new ArrayList<>();
			for (var inputStream : fsstreamfirst) {
				try (var input = new Input(inputStream);) {
					// while (input.available() > 0) {
					var inputdatas = (List) Utils.getKryo().readClassAndObject(input);
					// Get Streams object from list of map functions.
					try (var streammap = (BaseStream) StreamUtils.getFunctionsToStream(functions,
							inputdatas.parallelStream());) {
						if (function instanceof CalculateCount) {
							out = new Vector<>();
							if (streammap instanceof IntStream intstr) {
								out.add(intstr.count());
							} else {
								out.add(((Stream) streammap).count());
							}
						} else if (function instanceof PipelineIntStreamCollect piplineistream) {
							out = new Vector<>();
							out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
									piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

						} else if (function instanceof SummaryStatistics) {
							out = new Vector<>();
							out.add(((IntStream) streammap).summaryStatistics());

						} else if (function instanceof Max) {
							out = new Vector<>();
							out.add(((IntStream) streammap).max().getAsInt());

						} else if (function instanceof Min) {
							out = new Vector<>();
							out.add(((IntStream) streammap).min().getAsInt());

						} else if (function instanceof Sum) {
							out = new Vector<>();
							out.add(((IntStream) streammap).sum());

						} else if (function instanceof StandardDeviation) {
							out = new Vector<>();
							var streamtmp = (List) ((java.util.stream.IntStream) streammap).boxed()
									.collect(Collectors.toList());
							var mean = streamtmp.stream().mapToInt(Integer.class::cast).average().getAsDouble();
							var variance = (double) streamtmp.stream().mapToInt(Integer.class::cast)
									.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
							var standardDeviation = Math.sqrt(variance);
							out.add(standardDeviation);

						} else {
							out = (List) ((Stream) streammap).collect(Collectors.toCollection(Vector::new));
						}
					} catch (Exception ex) {
						log.error(PipelineConstants.PROCESSHDFSERROR, ex);
						throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
					}
					// }
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSHDFSERROR, ex);
					throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
				}
			}
			Utils.getKryo().writeClassAndObject(output, out);
			output.flush();

			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processBlockHDFSMap");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
			log.debug("GC Status Map task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		}
	}

	/**
	 * Process the sampling of data stream.
	 * @param numofsample
	 * @param blockslocation
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	@SuppressWarnings("unchecked")
	public double processSamplesBlocks(Integer numofsample, BlocksLocation blockslocation, FileSystem hdfs)
			throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processSamplesBlocks");
		var function = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var bais = getIntermediateInputStreamFS(blockslocation);
				var buffer = new BufferedReader(new InputStreamReader(bais));
				var stringdata = buffer.lines().parallel();) {

			// Limit the sample using the limit method.
			var terminalCount = false;
			if (function instanceof CalculateCount) {
				terminalCount = true;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(stringdata.limit(numofsample).count());
			} else {
				out = (List) stringdata.limit(numofsample).collect(Collectors.toCollection(Vector::new));
			}
			Utils.getKryo().writeClassAndObject(output, out);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processSamplesBlocks");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSSAMPLE, ex);
			throw new PipelineException(PipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	/**
	 * Process the sampling of data stream.
	 * @param numofsample
	 * @param fsstreams
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processSamplesObjects(Integer numofsample, List fsstreams) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processSamplesObjects");
		var function = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = new Input(
						new BufferedInputStream(((InputStream) (fsstreams.iterator().next())))
						);) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(datafirst.parallelStream().limit(numofsample).count());
			} else {
				// Limit the sample using the limit method.
				out = (List) datafirst.parallelStream().limit(numofsample)
						.collect(Collectors.toCollection(Vector::new));
			}
			Utils.getKryo().writeClassAndObject(output, out);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processSamplesObjects");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Sampling Task is " + timetaken + " seconds");
			log.debug("GC Status Sampling task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSSAMPLE, ex);
			throw new PipelineException(PipelineConstants.PROCESSSAMPLE, ex);
		}
	}

	/**
	 * Process the tasks calling this method.
	 */
	@Override
	public void run() {
		log.debug("Entered MassiveDataStreamTaskIgnite.call");
		var stagePartition = jobstage.getStageid();
		try {
			cache = ignite.getOrCreateCache(DataSamudayaConstants.DATASAMUDAYACACHE);

			if (task.input != null) {
				var numinputs = task.input.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
					var input = task.input[inputindex];
					if (!Objects.isNull(input) && input instanceof Task) {
						var inputtask = (Task) input;
						task.input[inputindex] = new ByteArrayInputStream(
								cache.get(inputtask.jobid + inputtask.stageid + inputtask.taskid));
					}
				}
			}
			computeTasks(task);
			log.info("Finished step: " + stagePartition);
			completed = true;
		} catch (Exception ex) {
			log.error("Failed stage: " + stagePartition, ex);
		}
		log.debug("Exiting MassiveDataStreamTaskIgnite.call");
	}

	/**
	 * Compute the various stage tasks such as union, intersection, map, filter,
	 * flatmap, reduce, reducebykey,coalesce, joins etc.
	 * @param task
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	public double computeTasks(Task task) throws Exception {
		var timetakenseconds = 0.0;
		if (jobstage.getStage().tasks.get(0) instanceof JoinPredicate jp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = (InputStream) streamfirst;
					var streamsecondtocompute = (InputStream) streamsecond;) {
				timetakenseconds = processJoinLZF(streamfirsttocompute, streamsecondtocompute, jp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
			}

		} else if (jobstage.getStage().tasks.get(0) instanceof LeftOuterJoinPredicate ljp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = (InputStream) streamfirst;
					var streamsecondtocompute = (InputStream) streamsecond;) {
				timetakenseconds = processLeftOuterJoinLZF(streamfirsttocompute, streamsecondtocompute, ljp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof RightOuterJoinPredicate rjp) {
			InputStream streamfirst = null;
			InputStream streamsecond = null;
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				streamfirst = HdfsBlockReader.getBlockDataInputStream(blfirst, hdfs);
				streamsecond = HdfsBlockReader.getBlockDataInputStream(blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof BlocksLocation)) {
				streamfirst = task.input[0] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[0], hdfs)
						: (InputStream) task.input[0];
				streamsecond = task.input[1] instanceof BlocksLocation
						? HdfsBlockReader.getBlockDataInputStream((BlocksLocation) task.input[1], hdfs)
						: (InputStream) task.input[1];
			} else {
				streamfirst = (InputStream) task.input[0];
				streamsecond = (InputStream) task.input[1];
			}
			try (var streamfirsttocompute = (InputStream) streamfirst;
					var streamsecondtocompute = (InputStream) streamsecond;) {
				timetakenseconds = processRightOuterJoinLZF(streamfirsttocompute, streamsecondtocompute, rjp,
						task.input[0] instanceof BlocksLocation, task.input[1] instanceof BlocksLocation);
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof IntersectionFunction) {

			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				timetakenseconds = processBlockHDFSIntersection(blfirst, blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof Blocks
							|| task.input[1] instanceof BlocksLocation)) {
				var streamfirst = new LinkedHashSet<InputStream>();
				var blockssecond = new ArrayList<BlocksLocation>();
				for (var input : task.input) {
					if (input instanceof InputStream instr) {
						streamfirst.add(instr);
					} else {
						if (input instanceof BlocksLocation blockslocation) {
							blockssecond.add(blockslocation);
						}
					}
				}
				timetakenseconds = processBlockHDFSIntersection(streamfirst, blockssecond, hdfs);
			} else if (task.input[0] instanceof InputStream && task.input[1] instanceof InputStream) {
				timetakenseconds = processBlockHDFSIntersection((List) Arrays.asList(task.input[0]),
						(List) Arrays.asList(task.input[1]));
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof UnionFunction) {
			if ((task.input[0] instanceof BlocksLocation blfirst)
					&& (task.input[1] instanceof BlocksLocation blsecond)) {
				timetakenseconds = processBlockHDFSUnion(blfirst, blsecond, hdfs);
			} else if (((task.input[0] instanceof BlocksLocation) && task.input[1] instanceof InputStream)
					|| ((task.input[0] instanceof InputStream) && task.input[1] instanceof Blocks
							|| task.input[1] instanceof BlocksLocation)) {
				var streamfirst = new LinkedHashSet<InputStream>();
				var blockssecond = new ArrayList<BlocksLocation>();
				for (var input : task.input) {
					if (input instanceof InputStream inputstr) {
						streamfirst.add(inputstr);
					} else {
						if (input instanceof BlocksLocation blockslocation) {
							blockssecond.add(blockslocation);
						}
					}
				}
				timetakenseconds = processBlockHDFSUnion(streamfirst, blockssecond, hdfs);
			} else if (task.input[0] instanceof InputStream && task.input[1] instanceof InputStream) {
				timetakenseconds = processBlockHDFSUnion((List) Arrays.asList(task.input[0]),
						(List) Arrays.asList(task.input[1]));
			}
		} else if (jobstage.getStage().tasks.get(0) instanceof IntSupplier sample) {
			var numofsample = sample.getAsInt();
			if (task.input[0] instanceof BlocksLocation bl) {
				timetakenseconds = processSamplesBlocks(numofsample, bl, hdfs);
			} else {
				timetakenseconds = processSamplesObjects(numofsample, (List) Arrays.asList(task.input));
			}
		} else if (task.input[0] instanceof BlocksLocation bl) {
			timetakenseconds = processBlockHDFSMap(bl, hdfs);
		} else if (jobstage.getStage().tasks.get(0) instanceof GroupByKeyFunction) {
			timetakenseconds = processGroupByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof FoldByKey) {
			timetakenseconds = processFoldByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof CountByKeyFunction) {
			timetakenseconds = processCountByKeyTuple2();
		} else if (jobstage.getStage().tasks.get(0) instanceof CountByValueFunction) {
			timetakenseconds = processCountByValueTuple2();
		} else if (task.input[0] instanceof InputStream) {
			if (jobstage.getStage().tasks.get(0) instanceof Coalesce) {
				timetakenseconds = processCoalesce();
			} else if (jobstage.getStage().tasks.get(0) instanceof HashPartitioner) {
				timetakenseconds = processHashPartition();
			} else if (jobstage.getStage().tasks.get(0) instanceof GroupByFunction) {
				timetakenseconds = processGroupBy();
			} else {
				var streams = new LinkedHashSet<InputStream>();
				streams.addAll((List) Arrays.asList(task.input));
				timetakenseconds = processBlockHDFSMap(streams);
			}
		}
		return timetakenseconds;
	}

	/**
	 * Join pair operation.
	 * @param streamfirst
	 * @param streamsecond
	 * @param joinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processJoinLZF(InputStream streamfirst, InputStream streamsecond, JoinPredicate joinpredicate,
			boolean isinputfirstblocks, boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskIgnite.processJoinLZF");
		var starttime = System.currentTimeMillis();

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null
						: new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null
						: new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			List inputs1 = null, inputs2 = null;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout.add(seq1.innerJoin(seq2.parallel(), joinpredicate).count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout = seq1.innerJoin(seq2.parallel(), joinpredicate).toList();
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}

			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Join task is " + timetaken + " seconds");
			log.debug("GC Status Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
		}
	}

	/**
	 * Left Outer Join computation
	 * @param streamfirst
	 * @param streamsecond
	 * @param leftouterjoinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processLeftOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			LeftOuterJoinPredicate leftouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processLeftOuterJoinLZF");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null
						: new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null
						: new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout.add(seq1.leftOuterJoin(seq2.parallel(), leftouterjoinpredicate).count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout = seq1.leftOuterJoin(seq2.parallel(), leftouterjoinpredicate).toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							joinpairsout = (List) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.filter(value -> {
										Tuple2 csvrec = (Tuple2) value;
										Object rec1 = csvrec.v1;
										Object rec2 = csvrec.v2;
										return rec1 instanceof CSVRecord && rec2 instanceof CSVRecord;
									}).map((Object rec) -> {
										try {
											Tuple2 csvrec = (Tuple2) rec;
											CSVRecord rec1 = (CSVRecord) csvrec.v1;
											CSVRecord rec2 = (CSVRecord) csvrec.v2;
											Map<String, String> keyvalue = rec1.toMap();
											keyvalue.putAll(rec2.toMap());
											List<String> keys = new ArrayList<>(keyvalue.keySet());
											CSVRecord recordmutated = CSVParser
													.parse(keyvalue.values().stream().collect(Collectors.joining(",")),
															CSVFormat.DEFAULT.withQuote('"').withEscape('\\')
																	.withHeader(keys.toArray(new String[keys.size()])))
													.getRecords().get(0);
											return recordmutated;
										} catch (IOException e) {

										}
										return null;
									}).collect(Collectors.toList());
						} else if (tuple2.v1 instanceof Map && (tuple2.v2 == null || tuple2.v2 instanceof Map)) {
							Map<String, Object> keyvaluemap = (Map<String, Object>) inputs2.get(0);
							Map<String, Object> nullmap = new HashMap<>();
							keyvaluemap.keySet().forEach(key -> nullmap.put(key, null));
							joinpairsout = (List) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.map(value -> {
										Tuple2 maprec = (Tuple2) value;
										Map<String, Object> rec1 = (Map<String, Object>) maprec.v1;
										Map<String, Object> rec2 = (Map<String, Object>) maprec.v2;
										if (rec2 == null) {
											return new Tuple2(rec1, nullmap);
										}
										return maprec;
									}).collect(Collectors.toList());
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processLeftOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Left Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Left Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
		}
	}

	/**
	 * Right outer join computation.
	 * @param streamfirst
	 * @param streamsecond
	 * @param rightouterjoinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processRightOuterJoinLZF(InputStream streamfirst, InputStream streamsecond,
			RightOuterJoinPredicate rightouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processRightOuterJoinLZF");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);
				var inputfirst = isinputfirstblocks ? null
						: new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null
						: new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

		) {

			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				inputs1 = buffreader1.lines().collect(Collectors.toList());
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				inputs2 = buffreader2.lines().collect(Collectors.toList());
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout.add(seq1.rightOuterJoin(seq2.parallel(), rightouterjoinpredicate).count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray()); var seq2 = Seq.of(inputs2.toArray());) {
					joinpairsout = seq1.rightOuterJoin(seq2.parallel(), rightouterjoinpredicate).toList();
					if (!joinpairsout.isEmpty()) {
			            Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
			            if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
			            	joinpairsout = (List) joinpairsout.stream()
			                  .filter(val -> val instanceof Tuple2).filter(value -> {
			                    Tuple2 csvrec = (Tuple2) value;
			                    Object rec1 = csvrec.v1;
			                    Object rec2 = csvrec.v2;
			                    return rec1 instanceof CSVRecord && rec2 instanceof CSVRecord;
			                  }).map((Object rec) -> {
			                    try {
			                      Tuple2 csvrec = (Tuple2) rec;
			                      CSVRecord rec1 = (CSVRecord) csvrec.v1;
			                      CSVRecord rec2 = (CSVRecord) csvrec.v2;
			                      Map<String, String> keyvalue = rec1.toMap();
			                      keyvalue.putAll(rec2.toMap());
			                      List<String> keys = new ArrayList<>(keyvalue.keySet());
			                      CSVRecord recordmutated =
			                          CSVParser
			                              .parse(keyvalue.values().stream().collect(Collectors.joining(",")),
			                                  CSVFormat.DEFAULT.withQuote('"').withEscape('\\')
			                                      .withHeader(keys.toArray(new String[keys.size()])))
			                              .getRecords().get(0);
			                      return recordmutated;
			                    } catch (IOException e) {

			                    }
			                    return null;
			                  })
			                  .collect(Collectors.toList());
			            }else if((tuple2.v1 == null || tuple2.v1 instanceof Map) 
			            		&& tuple2.v2 instanceof Map) {
			            	Map<String,Object> keyvaluemap = (Map<String, Object>) inputs1.get(0);
			            	Map<String,Object> nullmap = new HashMap<>();
			            	keyvaluemap.keySet().forEach(key->nullmap.put(key, null));
			            	joinpairsout = (List) joinpairsout.stream()
			                        .filter(val -> val instanceof Tuple2).map(value -> {
			                          Tuple2 maprec = (Tuple2) value;
			                          Map<String,Object> rec1 = (Map<String, Object>) maprec.v1;
			                          Map<String,Object> rec2 = (Map<String, Object>) maprec.v2;
			                          if(rec1 == null) {
			                        	  return new Tuple2(nullmap, rec2); 
			                          }
			                          return maprec;
			                        }).collect(Collectors.toList());
			            }
			          }
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processRightOuterJoinLZF");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Right Outer Join task is " + timetaken + " seconds");
			log.debug("GC Status Right Outer Join task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
			throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
		}
	}

	/**
	 * Group by key pair operation.
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processGroupByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processGroupByKeyTuple2");

		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos)) {

			var allpairs = new ArrayList<Tuple2>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = (InputStream) fs;
						var input = new Input(new BufferedInputStream(fsdis));) {
					// while (input.available() > 0) {
					var keyvaluepair = Utils.getKryo().readClassAndObject(input);
					if (keyvaluepair instanceof List kvp) {
						allpairs.addAll(kvp);
					} else if (keyvaluepair instanceof Map kvpmap) {
						mapgpbykey.add(kvpmap);
					}
					// }
				} catch (IOException ioe) {
					log.error(PipelineConstants.FILEIOERROR, ioe);
					throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedgroupbykey = (Map) Seq.of(allpairs.toArray(new Tuple2[allpairs.size()]))
						.groupBy(tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				var out = (List<Tuple2>) processedgroupbykey.keySet().parallelStream()
						.map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
						.collect(Collectors.toCollection(ArrayList::new));
				Utils.getKryo().writeClassAndObject(output, out);
			} else if (!mapgpbykey.isEmpty()) {
				Map result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				List<Tuple2> out = (List<Tuple2>) result.keySet().parallelStream()
						.map(key -> Tuple.tuple(key, result.get(key))).collect(Collectors.toCollection(ArrayList::new));
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				Utils.getKryo().writeClassAndObject(output, out);
			} else {
				Utils.getKryo().writeClassAndObject(output, new Vector<>());
			}
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processGroupByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Group By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Group By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
		}
	}

	/**
	 * Compute Fold by key pair operation.
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processFoldByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processFoldByKeyTuple2");

		var function = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);) {

			var allpairs = new ArrayList<Tuple2>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = (InputStream) fs;
						var input = new Input(new BufferedInputStream(fsdis));) {
					// while (input.available() > 0) {
					var keyvaluepair = Utils.getKryo().readClassAndObject(input);
					if (keyvaluepair instanceof List kvp) {
						allpairs.addAll(kvp);
					} else if (keyvaluepair instanceof Map kvpmap) {
						mapgpbykey.add(kvpmap);
					}
					// }
				} catch (IOException ioe) {
					log.error(PipelineConstants.FILEIOERROR, ioe);
					throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSGROUPBYKEY, ex);
					throw new PipelineException(PipelineConstants.PROCESSGROUPBYKEY, ex);
				}
			}
			// Parallel processing of fold by key operation.
			var foldbykey = (FoldByKey) jobstage.getStage().tasks.get(0);
			if (!allpairs.isEmpty()) {
				var finalfoldbykeyobj = new ArrayList<Tuple2>();
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(tup2 -> tup2.v1,
						Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				for (var key : processedgroupbykey.keySet()) {
					Object foldbykeyresult;
					if (foldbykey.isLeft()) {
						foldbykeyresult = Seq.of(processedgroupbykey.get(key).toArray()).foldLeft(foldbykey.getValue(),
								foldbykey.getReduceFunction());
					} else {
						foldbykeyresult = Seq.of(processedgroupbykey.get(key).toArray()).foldRight(foldbykey.getValue(),
								foldbykey.getReduceFunction());
					}
					finalfoldbykeyobj.add(Tuple.tuple(key, foldbykeyresult));
				}
				Utils.getKryo().writeClassAndObject(output, finalfoldbykeyobj);
			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
						.flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				var out = new ArrayList<>();
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				Utils.getKryo().writeClassAndObject(output, out);
			}
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processFoldByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Fold By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Fold By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSFOLDBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSFOLDBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	public double processCountByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processCountByKeyTuple2");
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);) {

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = (InputStream) fs;
				var input = new Input(new BufferedInputStream(fsdis));
				// while (input.available() > 0) {
				var keyvaluepair = Utils.getKryo().readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedcountbykey = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
				var intermediatelist = (List<Tuple2>) processedcountbykey.entrySet().parallelStream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(Collectors.toCollection(Vector::new));
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					intermediatelist = (List<Tuple2>) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream())).collect(Collectors.toCollection(Vector::new));
				}
				Utils.getKryo().writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processCountByKeyTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Key Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Key task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOUNTBYKEY, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOUNTBYKEY, ex);
		}
	}

	/**
	 * Count by key pair operation.
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	public double processCountByValueTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processCountByValueTuple2");
		try (var fsdos = createIntermediateDataToFS();
				var output = new Output(fsdos);) {

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = (InputStream) fs;
				var input = new Input(new BufferedInputStream(fsdis));
				// while (input.available() > 0) {
				var keyvaluepair = Utils.getKryo().readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedcountbyvalue = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
				var intermediatelist = (List<Tuple2>) processedcountbyvalue.entrySet().parallelStream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(Collectors.toCollection(Vector::new));
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					intermediatelist = (List<Tuple2>) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream())).collect(Collectors.toCollection(Vector::new));
				}

				Utils.getKryo().writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processCountByValueTuple2");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Count By Value Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOUNTBYVALUE, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOUNTBYVALUE, ex);
		}
	}

	/**
	 * Result of Coalesce by key operation.
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processCoalesce() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskIgnite.processCoalesce");
		var coalescefunction = (List<Coalesce>) getFunctions();
		try (var fsdos = createIntermediateDataToFS();
				var currentoutput = new Output(fsdos);) {
			var keyvaluepairs = new ArrayList<Tuple2>();
			for (var fs : task.input) {
				try (var fsos = (InputStream) fs;
						var input = new Input(new BufferedInputStream(fsos));) {
					// while (input.available() > 0) {
					keyvaluepairs.addAll((List) Utils.getKryo().readClassAndObject(input));
					// }
				}
			}
			List outpairs = null;
			if (Objects.nonNull(coalescefunction.get(0))
					&& Objects.nonNull(coalescefunction.get(0).getCoalescefunction())) {
				if (coalescefunction.get(0).getCoalescefunction() instanceof PipelineCoalesceFunction pcf) {
					outpairs = Arrays.asList(keyvaluepairs.parallelStream().reduce(pcf).get());
				} else {
					Map<Object,Object> cf = keyvaluepairs.parallelStream().collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
							(input1, input2) -> coalescefunction.get(0).getCoalescefunction().apply(input1, input2)));
					outpairs = (List) cf.entrySet().parallelStream()
							.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
							.collect(Collectors.toCollection(Vector::new));
				}
			}
			var functions = getFunctions();
			if (functions.size() > 1) {
				functions.remove(0);
				var finaltask = functions.get(functions.size() - 1);
				var stream = StreamUtils.getFunctionsToStream(functions, outpairs.parallelStream());
				if (finaltask instanceof CalculateCount) {
					outpairs = new Vector<>();
					if (stream instanceof IntStream intstr) {
						outpairs.add(intstr.count());
					} else {
						outpairs.add(((Stream) stream).count());
					}
				} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
					outpairs = new Vector<>();
					outpairs.add(((IntStream) stream).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else {
					outpairs = (List) ((Stream) stream).collect(Collectors.toCollection(Vector::new));
				}
			}
			Utils.getKryo().writeClassAndObject(currentoutput, outpairs);
			currentoutput.flush();

			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting MassiveDataStreamTaskIgnite.processCoalesce");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Coalesce Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSCOALESCE, ex);
			throw new PipelineException(PipelineConstants.PROCESSCOALESCE, ex);
		}
	}
	
	
	/**
	 * Result of HashPartition by key operation
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processHashPartition() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processHashPartition");
		var hashpartition = (List<HashPartitioner>) getFunctions();
		try (var fsdos = createIntermediateDataToFS(); var currentoutput = new Output(fsdos);) {

			var keyvaluepairs = new ArrayList<Tuple2>();
			for (var fs : task.input) {
				try (var fsis = (InputStream) fs; var input = new Input(fsis);) {
					keyvaluepairs.addAll((List) Utils.getKryo().readClassAndObject(input));
				}
			}
			log.debug("Data Size:" + keyvaluepairs.size());
			// Parallel execution of reduce by key stream execution.
			List output = null;
			if (Objects.nonNull(hashpartition.get(0))) {
				int partitionnumber = hashpartition.get(0).getPartitionnumber();
				Map<Integer, List<Tuple2>> mappartitoned = keyvaluepairs.parallelStream()
						.collect(Collectors.groupingBy(tup2 -> tup2.v1.hashCode() % partitionnumber, HashMap::new,
								Collectors.mapping(tup2 -> tup2, Collectors.toList())));
				output = new ArrayList<Tuple2<Integer, List<Tuple2>>>();
				for (int partitionindex = 0; partitionindex < partitionnumber; partitionindex++) {
					output.add(new Tuple2<Integer, List<Tuple2>>(Integer.valueOf(partitionindex),
							mappartitoned.get(partitionindex)));
				}

			}
			Utils.getKryo().writeClassAndObject(currentoutput, output);
			currentoutput.flush();
			cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
			log.debug("Exiting StreamPipelineTaskExecutor.processHashPartition");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Coalesce Task is " + timetaken + " seconds");
			log.debug("GC Status Count By Value task:" + Utils.getGCStats());
			return timetaken;
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHASHPARTITION, ex);
			throw new PipelineException(PipelineConstants.PROCESSHASHPARTITION, ex);
		}
	}
	
	
	/**
	   * Result of Group By operation
	   * @return timetaken in seconds
	   * @throws PipelineException
	   */
	  @SuppressWarnings("unchecked")
	  public double processGroupBy() throws PipelineException {
	    var starttime = System.currentTimeMillis();
	    log.debug("Entered StreamPipelineTaskExecutor.processHashPartition");
	    var groupbyfunctions = (List<GroupByFunction>) getFunctions();
	    var fsdos = new ByteArrayOutputStream();
	    try (var currentoutput = new Output(fsdos);) {

	      var keyvaluepairs = new ArrayList<>();
	      for (var fs : task.input) {
	        try (var fsis = (InputStream) fs;
	            var input = new Input(fsis);) {
	          keyvaluepairs.addAll((List) Utils.getKryo().readClassAndObject(input));
	        }
	      }
	      log.debug("Data Size:" + keyvaluepairs.size());
	      // Parallel execution of reduce by key stream execution.
	      List output = null;
	      if (Objects.nonNull(groupbyfunctions.get(0))) {
	    	  GroupByFunction gbf = groupbyfunctions.get(0);
	          Map<Object, List<Object>> mapgroupby =
	              keyvaluepairs.parallelStream()
	                  .collect(Collectors.groupingBy(
	                		  obj->gbf.apply(obj), 
	                		  HashMap::new,
	                		  Collectors.mapping(obj->obj, Collectors.toList())));
	          output = mapgroupby.keySet().stream()
	          .map(key->new Tuple2<Object,List<Object>>(key,mapgroupby.get(key)))
	          .collect(Collectors.toList());
	          
	        }     
	      Utils.getKryo().writeClassAndObject(currentoutput,output);
	      currentoutput.flush();
	      cache.put(task.jobid + task.stageid + task.taskid, fsdos.toByteArray());
	      log.debug("Exiting StreamPipelineTaskExecutor.processHashPartition");
	      var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
	      log.debug("Time taken to compute the Coalesce Task is " + timetaken + " seconds");
	      log.debug("GC Status Count By Value task:" + Utils.getGCStats());
	      return timetaken;
	    } catch (IOException ioe) {
	      log.error(PipelineConstants.FILEIOERROR, ioe);
	      throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
	    } catch (Exception ex) {
	      log.error(PipelineConstants.PROCESSHASHPARTITION, ex);
	      throw new PipelineException(PipelineConstants.PROCESSHASHPARTITION, ex);
	    }
	  }
	
	
}
