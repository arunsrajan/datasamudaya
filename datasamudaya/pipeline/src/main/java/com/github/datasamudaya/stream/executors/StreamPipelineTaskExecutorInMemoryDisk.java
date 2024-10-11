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

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.charset.StandardCharsets;
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
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.log4j.Logger;
import org.ehcache.Cache;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.ByteBufferPoolDirectOld;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.functions.FoldByKey;
import com.github.datasamudaya.common.functions.GroupByFunction;
import com.github.datasamudaya.common.functions.HashPartitioner;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.PipelineCoalesceFunction;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.CsvOptions;
import com.github.datasamudaya.stream.Json;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.PipelineIntStreamCollect;
import com.github.datasamudaya.stream.utils.StreamUtils;
import com.pivovarit.collectors.ParallelCollectors;

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public sealed class StreamPipelineTaskExecutorInMemoryDisk extends StreamPipelineTaskExecutorInMemory permits StreamPipelineTaskExecutorInMemoryDiskSQL {
	private static Logger log = Logger.getLogger(StreamPipelineTaskExecutorInMemoryDisk.class);

	public StreamPipelineTaskExecutorInMemoryDisk(JobStage jobstage, ConcurrentMap<String, OutputStream> resultstream,
			Cache cache) throws Exception {
		super(jobstage, resultstream, cache);
		iscacheable = true;
	}

	/**
	 * This method gets the path in jobid-stageid-taskid.
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}

	/**
	 * Creates OutputStream from task object and buffer size.
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task, int buffersize) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.createIntermediateDataToFS");
		try {
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirectOld.get(buffersize));
			log.debug("Exiting StreamPipelineTaskExecutorInMemoryDisk.createIntermediateDataToFS");
			return os;
		} catch (Exception e) {
			log.error(PipelineConstants.FILEIOERROR, e);
			throw new PipelineException(PipelineConstants.FILEIOERROR, e);
		}
	}

	/**
	 * Gets the byte array stored as intermediate data from cache object.
	 * 
	 * @param rdf
	 * @returns cached bytes
	 * @throws Exception
	 */
	public byte[] getCachedRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.getIntermediateInputStreamRDF");
		var path = rdf.getJobid() + DataSamudayaConstants.HYPHEN + rdf.getStageid() + DataSamudayaConstants.HYPHEN + rdf.getTaskid();
		return (byte[]) cache.get(path);
	}

	/**
	 * get intermediate tasks outputs from cache.
	 * 
	 * @param task
	 * @return intermediate output in bytes from cache
	 * @throws Exception
	 */
	public byte[] getCachedTask(Task task) throws Exception {
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.getIntermediateInputStreamTask");
		var path = task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid();
		return (byte[]) cache.get(path);
	}

	/**
	 * Process the data using intersection function.
	 * 
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	public double processBlockHDFSIntersection(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws Exception {
		long starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);

				var bais1 = HdfsBlockReader.getBlockDataInputStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines();
				var streamsecond = buffer2.lines();) {
			var cf = (CompletableFuture) streamsecond.distinct().collect(ParallelCollectors.parallel(value -> value,
					Collectors.toCollection(LinkedHashSet::new), executor, Runtime.getRuntime().availableProcessors()));
			var setsecond = (Set) cf.get();
			// Get the result of intersection functions parallel.
			cf = (CompletableFuture) streamfirst.distinct().filter(setsecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));

			Object result = cf.get();
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					Utils.convertToCsv((List) result, os);
				}
				return (System.currentTimeMillis() - starttime) / 1000.0;
			}
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSIntersection");
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
	 * 
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws Exception
	 */
	@SuppressWarnings({"unchecked"})
	public double processBlockHDFSIntersection(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws Exception {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = new Input(fsstreamfirst.iterator().next());
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines();) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var cf = (CompletableFuture) streamsecond.distinct().collect(ParallelCollectors.parallel(value -> value,
					Collectors.toCollection(LinkedHashSet::new), executor, Runtime.getRuntime().availableProcessors()));
			;
			var setsecond = (Set) cf.get();
			// Parallel execution of the intersection function.
			cf = (CompletableFuture) datafirst.stream().distinct().filter(setsecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));
			Object result = cf.get();
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					Utils.convertToCsv((List) result, os);
				}
				return (System.currentTimeMillis() - starttime) / 1000.0;
			}
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSIntersection");
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
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @return timetaken in seconds.
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public double processBlockHDFSIntersection(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSIntersection");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = new Input(fsstreamfirst.iterator().next());
				var inputsecond = new Input(fsstreamsecond.iterator().next());

				) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var datasecond = (List) Utils.getKryo().readClassAndObject(inputsecond);
			// parallel execution of intersection function.
			var cf = (CompletableFuture) datafirst.stream().distinct().filter(datasecond::contains)
					.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
							Runtime.getRuntime().availableProcessors()));
			Object result = cf.get();
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					Utils.convertToCsv((List) result, os);
				}
				return (System.currentTimeMillis() - starttime) / 1000.0;
			}
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<Object>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSIntersection");
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
	 * Open the already existing file using the job id and task id.
	 * 
	 * @return file input stream
	 * @throws Exception
	 */
	public InputStream getIntermediateInputStreamFS(Task task) throws Exception {
		var path = getIntermediateDataFSFilePath(task);
		return new FileInputStream(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TMPDIR) + path);
	}

	/**
	 * Perform the union operation.
	 * 
	 * @param blocksfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public double processBlockHDFSUnion(BlocksLocation blocksfirst, BlocksLocation blockssecond, FileSystem hdfs)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var bais1 = HdfsBlockReader.getBlockDataInputStream(blocksfirst, hdfs);
				var buffer1 = new BufferedReader(new InputStreamReader(bais1));
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond, hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamfirst = buffer1.lines();
				var streamsecond = buffer2.lines();) {
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			List result;
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + Stream.concat(streamfirst, streamsecond).distinct().count())
								.getBytes());
					} else {
						Stream.concat(streamfirst, streamsecond).distinct().forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			if (terminalCount) {
				result = new Vector<>();
				result.add(Stream.concat(streamfirst, streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) Stream.concat(streamfirst, streamsecond).distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}

			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSUnion");
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
	 * 
	 * @param fsstreamfirst
	 * @param blockssecond
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public double processBlockHDFSUnion(Set<InputStream> fsstreamfirst, List<BlocksLocation> blockssecond,
			FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = new Input(fsstreamfirst.iterator().next());
				var bais2 = HdfsBlockReader.getBlockDataInputStream(blockssecond.get(0), hdfs);
				var buffer2 = new BufferedReader(new InputStreamReader(bais2));
				var streamsecond = buffer2.lines();) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write((""
								+ Stream.concat(datafirst.stream(), streamsecond).distinct().count())
								.getBytes());
					} else {
						Stream.concat(datafirst.stream(), streamsecond).distinct().forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List result;
			if (terminalCount) {
				result = new Vector<>();
				result.add(Stream.concat(datafirst.stream(), streamsecond).distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) Stream.concat(datafirst.stream(), streamsecond).distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}
			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSUnion");
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
	 * 
	 * @param fsstreamfirst
	 * @param fsstreamsecond
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings({"unchecked"})
	public double processBlockHDFSUnion(List<InputStream> fsstreamfirst, List<InputStream> fsstreamsecond)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSUnion");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = new Input(fsstreamfirst.iterator().next());
				var inputsecond = new Input(fsstreamsecond.iterator().next());) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var datasecond = (List) Utils.getKryo().readClassAndObject(inputsecond);
			List result;
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + Stream.concat(datafirst.stream(), datasecond.stream())
								.distinct().count()).getBytes());
					} else {
						Stream.concat(datafirst.stream(), datasecond.stream()).distinct()
								.forEach(val -> {
									try {
										os.write(val.toString().getBytes());
										os.write(ch);
									} catch (IOException e) {
									}
								});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			if (terminalCount) {
				result = new ArrayList<>();
				result.add(Stream.concat(datafirst.parallelStream(), datasecond.parallelStream())
						.distinct().count());
			} else {
				// parallel stream union operation result
				var cf = (CompletableFuture) Stream.concat(datafirst.stream(), datasecond.stream())
						.distinct()
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				result = (List) cf.get();
			}

			Utils.getKryo().writeClassAndObject(output, result);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(result);
			result = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSUnion");
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
	 * 
	 * @param blockslocation
	 * @param hdfs
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(BlocksLocation blockslocation, FileSystem hdfs) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSMap");
		CSVParser records = null;

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));) {

			Stream intermediatestreamobject;
			if (jobstage.getStage().tasks.get(0) instanceof Json) {
				intermediatestreamobject = buffer.lines();
				intermediatestreamobject = intermediatestreamobject.map(line -> {
					try {
						return new JSONParser().parse((String) line);
					} catch (ParseException e) {
						return null;
					}
				});
			} else {
				if (jobstage.getStage().tasks.get(0) instanceof CsvOptions csvoptions) {
					var csvformat = CSVFormat.DEFAULT.withQuote('"').withEscape('\\');
					csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader()).withIgnoreHeaderCase()
							.withTrim();

					try {
						records = csvformat.parse(buffer);
						Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(), false);
						intermediatestreamobject = streamcsv;
					} catch (IOException ioe) {
						log.error(PipelineConstants.FILEIOERROR, ioe);
						throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
					} catch (Exception ex) {
						log.error(PipelineConstants.PROCESSHDFSERROR, ex);
						throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
					}
				} else {
					intermediatestreamobject = buffer.lines();
				}

			}
			intermediatestreamobject.onClose(() -> {
				log.debug("Stream closed");
			});
			var finaltask = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);

			try (var streammap = (BaseStream) StreamUtils.getFunctionsToStream(getFunctions(),
					intermediatestreamobject);) {
				List out;

				if (finaltask instanceof CalculateCount) {
					out = new Vector<>();
					if (streammap instanceof IntStream stmap) {
						out.add(stmap.count());
					} else {
						out.add(((Stream) streammap).count());
					}
				} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
					out = new Vector<>();
					out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else if (finaltask instanceof SummaryStatistics) {
					out = new Vector<>();
					out.add(((IntStream) streammap).summaryStatistics());

				} else if (finaltask instanceof Max) {
					out = new Vector<>();
					out.add(((IntStream) streammap).max().getAsInt());

				} else if (finaltask instanceof Min) {
					out = new Vector<>();
					out.add(((IntStream) streammap).min().getAsInt());

				} else if (finaltask instanceof Sum) {
					out = new Vector<>();
					out.add(((IntStream) streammap).sum());

				} else if (finaltask instanceof StandardDeviation) {
					out = new Vector<>();
					CompletableFuture<List> cf = (CompletableFuture) ((IntStream) streammap).boxed()
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					var streamtmp = cf.get();
					var mean = streamtmp.stream().mapToInt(Integer.class::cast).average().getAsDouble();
					var variance = streamtmp.stream().mapToInt(Integer.class::cast)
							.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
					var standardDeviation = Math.sqrt(variance);
					out.add(standardDeviation);

				} else {
					log.debug("Map assembly deriving");
					CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					out = cf.get();
					if (task.finalphase && task.saveresulttohdfs) {
						try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
								Short.parseShort(DataSamudayaProperties.get().getProperty(
										DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
										DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
							Utils.convertToCsv((List) out, os);
						}
						return (System.currentTimeMillis() - starttime) / 1000.0;
					}
					log.debug("Map assembly concluded");
				}
				Utils.getKryo().writeClassAndObject(output, out);
				output.flush();
				task.setNumbytesgenerated(fsdos.toByteArray().length);
				cacheAble(fsdos);
				var wr = new WeakReference<List>(out);
				out = null;
				log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSMap");
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
		} catch (IOException ioe) {
			log.error(PipelineConstants.FILEIOERROR, ioe);
			throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		} finally {
			if (!Objects.isNull(records)) {
				try {
					records.close();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
		}

	}

	/**
	 * Write the intermediate in ehcache.
	 * 
	 * @param fsdos
	 */
	@SuppressWarnings("unchecked")
	public void cacheAble(OutputStream fsdos) {
		if (iscacheable) {
			byte[] bt = ((ByteArrayOutputStream) fsdos).toByteArray();
			cache.put(getIntermediateDataFSFilePath(task), bt);
		}
	}

	/**
	 * Perform map operation to obtain intermediate stage result.
	 * 
	 * @param fsstreamfirst
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processBlockHDFSMap(Set<InputStream> fsstreamfirst) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processBlockHDFSMap");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {

			var functions = getFunctions();

			List out = new ArrayList<>();
			var finaltask = jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1);
			for (var is : fsstreamfirst) {
				try (var input = new Input(is);) {
					// while (input.available() > 0) {
					var inputdatas = (List) Utils.getKryo().readClassAndObject(input);
					// Get Streams object from list of map functions.
					try (BaseStream streammap = (BaseStream) StreamUtils.getFunctionsToStream(functions,
							inputdatas.stream());) {
						if (finaltask instanceof CalculateCount) {
							out = new Vector<>();
							if (streammap instanceof IntStream stmap) {
								out.add(stmap.count());
							} else {
								out.add(((Stream) streammap).count());
							}
						} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
							out = new Vector<>();
							out.add(((IntStream) streammap).collect(piplineistream.getSupplier(),
									piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

						} else if (finaltask instanceof SummaryStatistics) {
							out = new Vector<>();
							out.add(((IntStream) streammap).summaryStatistics());

						} else if (finaltask instanceof Max) {
							out = new Vector<>();
							out.add(((IntStream) streammap).max().getAsInt());

						} else if (finaltask instanceof Min) {
							out = new Vector<>();
							out.add(((IntStream) streammap).min().getAsInt());

						} else if (finaltask instanceof Sum) {
							out = new Vector<>();
							out.add(((IntStream) streammap).sum());

						} else if (finaltask instanceof StandardDeviation) {
							out = new Vector<>();
							CompletableFuture<List> cf = (CompletableFuture) ((IntStream) streammap)
									.boxed()
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							var streamtmp = cf.get();
							double mean = streamtmp.stream().mapToInt(Integer.class::cast).average().getAsDouble();
							double variance = (double) streamtmp.stream().mapToInt(Integer.class::cast)
									.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
							double standardDeviation = Math.sqrt(variance);
							out.add(standardDeviation);

						} else {
							CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap).collect(
									ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
											executor, Runtime.getRuntime().availableProcessors()));
							out = cf.get();
							if (task.finalphase && task.saveresulttohdfs) {
								try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
										Short.parseShort(DataSamudayaProperties.get().getProperty(
												DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
												DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
									Utils.convertToCsv((List) out, os);
								}
								return (System.currentTimeMillis() - starttime) / 1000.0;
							}
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
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processBlockHDFSMap");
			var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
			log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
			log.debug("GC Status Map task:" + Utils.getGCStats());
			return timetaken;
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		}
	}

	/**
	 * The sampling done by this method
	 * 
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
		log.debug("Entered StreamPipelineTaskExecutor.processSamplesBlocks");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
				var buffer = new BufferedReader(new InputStreamReader(bais));
				var stringdata = buffer.lines();) {
			// Limit the sample using the limit method.
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(jobstage.getStage().tasks.size() - 1) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + stringdata.limit(numofsample).count()).getBytes());
					} else {
						stringdata.limit(numofsample).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(stringdata.limit(numofsample).count());
			} else {
				CompletableFuture<List> cf = (CompletableFuture) stringdata.limit(numofsample)
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				out = cf.get();
			}
			Utils.getKryo().writeClassAndObject(output, out);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processSamplesBlocks");
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
	 * Obtain data samples using this method.
	 * 
	 * @param numofsample
	 * @param fsstreams
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processSamplesObjects(Integer numofsample, List fsstreams) throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processSamplesObjects");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = new Input((InputStream) fsstreams.iterator().next());) {

			var datafirst = (List) Utils.getKryo().readClassAndObject(inputfirst);
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + datafirst.parallelStream().limit(numofsample).count()).getBytes());
					} else {
						datafirst.parallelStream().limit(numofsample).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List out;
			if (terminalCount) {
				out = new Vector<>();
				out.add(datafirst.parallelStream().limit(numofsample).count());
			} else {
				// Limit the sample using the limit method.
				CompletableFuture<List> cf = (CompletableFuture) datafirst.stream().limit(numofsample)
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				out = cf.get();
			}
			Utils.getKryo().writeClassAndObject(output, out);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(out);
			out = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processSamplesObjects");
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
	 * This method call computes the tasks from stages and return whether the tasks
	 * are computed successfully.
	 */
	@Override
	public Boolean call() {
		starttime = System.currentTimeMillis();
		task.taskexecutionstartime = starttime;
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDisk.call");
		var stageTasks = getStagesTask();
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		var configuration = new Configuration();
		var timetakenseconds = 0.0;
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {

			endtime = System.currentTimeMillis();
			log.debug("Submitted JobStage " + task.jobid + " " + task.stageid + " " + jobstage);
			log.debug("Running Stage " + stageTasks);
			if (task.input != null && task.parentremotedatafetch != null) {
				if (task.parentremotedatafetch != null && task.parentremotedatafetch[0] != null) {
					var numinputs = task.parentremotedatafetch.length;
					for (var inputindex = 0;inputindex < numinputs;inputindex++) {
						var input = task.parentremotedatafetch[inputindex];
						if (input != null) {
							var rdf = (RemoteDataFetch) input;
							var btarray = getCachedRDF(rdf);
							if (btarray != null) {
								task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(btarray));
							} else {
								RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
								task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(rdf.getData()));
							}
						}
					}
				} else if (task.input != null && task.input[0] != null) {
					var numinputs = task.input.length;
					for (var inputindex = 0;inputindex < numinputs;inputindex++) {
						var input = task.input[inputindex];
						if (input != null && input instanceof Task taskinput) {
							var btarray = getCachedTask(taskinput);
							if (btarray != null) {
								task.input[inputindex] = new SnappyInputStream(new ByteArrayInputStream(btarray));
							}
						}
					}
				}
			}
			log.debug("Running Stage " + task.jobid + " " + task.stageid + " " + jobstage);
			task.timetakenseconds = timetakenseconds = computeTasks(task, hdfs);
			completed = true;
			endtime = System.currentTimeMillis();
			task.taskexecutionendtime = endtime;
			log.debug("Completed JobStage " + task.jobid + " " + task.stageid + " in " + timetakenseconds);
		} catch (Throwable ex) {
			completed = false;
			log.error("Failed Stage: " + task.stageid, ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				task.taskexecutionendtime = endtime = System.currentTimeMillis();
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
		log.debug("Exiting StreamPipelineTaskExecutorInMemoryDisk.call");
		return completed;
	}

	/**
	 * Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutor.processJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs1.parallelStream().flatMap(tup1 -> {
				return inputs2.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup1.v2, tup2.v2))).collect(Collectors.toList())
						.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processJoin");
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
	 * Left Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processLeftJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutor.processLeftJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs1.parallelStream().flatMap(tup1 -> {
				List<Tuple2> joinlist = inputs2.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup1.v2, tup2.v2))).collect(Collectors.toList());
				if (joinlist.isEmpty()) {
					return Arrays.asList(new Tuple2(tup1.v1, new Tuple2(tup1.v2, null))).stream();
				}
				return joinlist.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processLeftJoin");
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
	 * Left Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in milliseconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processRightJoin(InputStream streamfirst, InputStream streamsecond, boolean isinputfirstblocks,
			boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutor.processRightJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			final List<Tuple2> inputs1, inputs2;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}

			Stream<Tuple2> joinpairs = inputs2.parallelStream().flatMap(tup1 -> {
				List<Tuple2> joinlist = inputs1.parallelStream().filter(tup2 -> tup1.v1.equals(tup2.v1))
						.map(tup2 -> new Tuple2(tup2.v1, new Tuple2(tup2.v2, tup1.v2))).collect(Collectors.toList());
				if (joinlist.isEmpty()) {
					return Arrays.asList(new Tuple2(tup1.v1, new Tuple2(null, tup1.v2))).stream();
				}
				return joinlist.stream();
			});

			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + joinpairs.count()).getBytes());
					} else {
						joinpairs.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try {
					joinpairsout.add(joinpairs.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				joinpairsout = joinpairs.collect(Collectors.toList());

			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processRightJoin");
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
	 * Join pair operation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param joinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds.
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processJoin(InputStream streamfirst, InputStream streamsecond, JoinPredicate joinpredicate,
			boolean isinputfirstblocks, boolean isinputsecondblocks) throws PipelineException {
		log.debug("Entered StreamPipelineTaskExecutor.processJoin");
		var starttime = System.currentTimeMillis();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			var terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqinnerjoin.count()).getBytes());
					} else {
						seqinnerjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate)) {
					joinpairsout.add(seqinnerjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqinnerjoin = seq1.innerJoin(seq2, joinpredicate)) {
					joinpairsout = seqinnerjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
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
							})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List<Map<String, String>>) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSJOIN, ex);
				}

			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processJoin");
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
	 * Function for Left Outer Join computation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param leftouterjoinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processLeftOuterJoin(InputStream streamfirst, InputStream streamsecond,
			LeftOuterJoinPredicate leftouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processLeftOuterJoin");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqleftouterjoin.count()).getBytes());
					} else {
						seqleftouterjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate)) {
					joinpairsout.add(seqleftouterjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqleftouterjoin = seq1.leftOuterJoin(seq2, leftouterjoinpredicate)) {
					joinpairsout = seqleftouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
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
							})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						} else if (tuple2.v1 instanceof Map && (tuple2.v2 == null || tuple2.v2 instanceof Map)) {
							Map<String, Object> keyvaluemap = (Map<String, Object>) inputs2.get(0);
							Map<String, Object> nullmap = new HashMap<>();
							keyvaluemap.keySet().forEach(key -> nullmap.put(key, null));
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.map(value -> {
										Tuple2 maprec = (Tuple2) value;
										Map<String, Object> rec1 = (Map<String, Object>) maprec.v1;
										Map<String, Object> rec2 = (Map<String, Object>) maprec.v2;
										if (rec2 == null) {
											return new Tuple2(rec1, nullmap);
										}
										return maprec;
									})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSLEFTOUTERJOIN, ex);
				}
			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processLeftOuterJoin");
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
	 * Function for Right Outer Join computation.
	 * 
	 * @param streamfirst
	 * @param streamsecond
	 * @param rightouterjoinpredicate
	 * @param isinputfirstblocks
	 * @param isinputsecondblocks
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processRightOuterJoin(InputStream streamfirst, InputStream streamsecond,
			RightOuterJoinPredicate rightouterjoinpredicate, boolean isinputfirstblocks, boolean isinputsecondblocks)
			throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processRightOuterJoin");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);
				var inputfirst = isinputfirstblocks ? null : new Input(streamfirst);
				var inputsecond = isinputsecondblocks ? null : new Input(streamsecond);
				var buffreader1 = isinputfirstblocks ? new BufferedReader(new InputStreamReader(streamfirst)) : null;
				var buffreader2 = isinputsecondblocks ? new BufferedReader(new InputStreamReader(streamsecond)) : null;

				) {

			List inputs1 = null, inputs2 = null;
			;
			if (Objects.isNull(buffreader1)) {
				inputs1 = (List) Utils.getKryo().readClassAndObject(inputfirst);
			} else {
				CompletableFuture<List> cf = buffreader1.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs1 = cf.get();
			}
			if (Objects.isNull(buffreader2)) {
				inputs2 = (List) Utils.getKryo().readClassAndObject(inputsecond);
			} else {
				CompletableFuture<List> cf = buffreader2.lines().collect(ParallelCollectors.parallel(value -> value,
						Collectors.toCollection(Vector::new), executor, Runtime.getRuntime().availableProcessors()));
				inputs2 = cf.get();
			}
			boolean terminalCount = false;
			if (jobstage.getStage().tasks.get(0) instanceof CalculateCount) {
				terminalCount = true;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
						var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate);) {
					int ch = (int) '\n';
					if (terminalCount) {
						os.write(("" + seqrightouterjoin.count()).getBytes());
					} else {
						seqrightouterjoin.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			List joinpairsout;
			if (terminalCount) {
				joinpairsout = new Vector<>();
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate)) {
					joinpairsout.add(seqrightouterjoin.count());
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			} else {
				// Parallel join pair result.
				try (var seq1 = Seq.of(inputs1.toArray());
						var seq2 = Seq.of(inputs2.toArray());
						var seqrightouterjoin = seq1.rightOuterJoin(seq2, rightouterjoinpredicate)) {
					joinpairsout = seqrightouterjoin.toList();
					if (!joinpairsout.isEmpty()) {
						Tuple2 tuple2 = (Tuple2) joinpairsout.get(0);
						if (tuple2.v1 instanceof CSVRecord && tuple2.v2 instanceof CSVRecord) {
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
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
							})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						} else if ((tuple2.v1 == null || tuple2.v1 instanceof Map) && tuple2.v2 instanceof Map) {
							Map<String, Object> keyvaluemap = (Map<String, Object>) inputs1.get(0);
							Map<String, Object> nullmap = new HashMap<>();
							keyvaluemap.keySet().forEach(key -> nullmap.put(key, null));
							var cf = (CompletableFuture) joinpairsout.stream().filter(val -> val instanceof Tuple2)
									.map(value -> {
										Tuple2 maprec = (Tuple2) value;
										Map<String, Object> rec1 = (Map<String, Object>) maprec.v1;
										Map<String, Object> rec2 = (Map<String, Object>) maprec.v2;
										if (rec1 == null) {
											return new Tuple2(nullmap, rec2);
										}
										return maprec;
									})
									.collect(ParallelCollectors.parallel(value -> value,
											Collectors.toCollection(Vector::new), executor,
											Runtime.getRuntime().availableProcessors()));
							joinpairsout = (List) cf.get();
						}
					}
				} catch (Exception ex) {
					log.error(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
					throw new PipelineException(PipelineConstants.PROCESSRIGHTOUTERJOIN, ex);
				}
			}
			Utils.getKryo().writeClassAndObject(output, joinpairsout);
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(joinpairsout);
			joinpairsout = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processRightOuterJoin");
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
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processGroupByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processGroupByKeyTuple2");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {

			var allpairs = new ArrayList<>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = (InputStream) fs; var input = new Input(fsdis);) {
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
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(
								tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
						processedgroupbykey.keySet().stream().map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
								.forEach(val -> {
									try {
										os.write(val.toString().getBytes());
										os.write(ch);
									} catch (IOException e) {
									}
								});

					} else if (!mapgpbykey.isEmpty()) {
						var result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
								.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors.mapping(
										(Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
						result.keySet().stream().map(key -> Tuple.tuple(key, result.get(key))).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});

					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			if (!allpairs.isEmpty()) {
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(tup2 -> tup2.v1,
						Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				var cf = (CompletableFuture) processedgroupbykey.keySet().stream()
						.map(key -> Tuple.tuple(key, processedgroupbykey.get(key)))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				Utils.getKryo().writeClassAndObject(output, cf.get());
			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map) mapgpbykey.parallelStream().flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
				var cf = (CompletableFuture) result.keySet().stream().map(key -> Tuple.tuple(key, result.get(key)))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				var out = (List) cf.get();
				result.keySet().parallelStream().forEach(key -> out.add(Tuple.tuple(key, result.get(key))));
				Utils.getKryo().writeClassAndObject(output, out);
			} else {
				Utils.getKryo().writeClassAndObject(output, new Vector<>());
			}
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			log.debug("Exiting StreamPipelineTaskExecutor.processGroupByKeyTuple2");
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
	 * Fold by key pair operation.
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processFoldByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processFoldByKeyTuple2");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {

			var allpairs = new ArrayList<Tuple2>();
			var mapgpbykey = new LinkedHashSet<Map>();
			for (var fs : task.input) {
				try (var fsdis = (InputStream) fs; var input = new Input(fsdis);) {
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
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(
								tup2 -> tup2.v1, Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
						for (var key : processedgroupbykey.keySet()) {
							var seqtuple2 = Seq.of(processedgroupbykey.get(key).toArray());
							Object foldbykeyresult;
							if (foldbykey.isLeft()) {
								foldbykeyresult = seqtuple2.foldLeft(foldbykey.getValue(),
										foldbykey.getReduceFunction());
							} else {
								foldbykeyresult = seqtuple2.foldRight(foldbykey.getValue(),
										foldbykey.getReduceFunction());
							}
							os.write(Tuple.tuple(key, foldbykeyresult).toString().getBytes());
							os.write(ch);

						}
					} else if (!mapgpbykey.isEmpty()) {
						var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
								.flatMap(map1 -> map1.entrySet().parallelStream())
								.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors.mapping(
										(Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));
						result.keySet().parallelStream().forEach(key -> {
							try {
								os.write(Tuple.tuple(key, result.get(key)).toString().getBytes());
								os.write(ch);
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}

			if (!allpairs.isEmpty()) {
				var finalfoldbykeyobj = new ArrayList<>();
				var processedgroupbykey = Seq.of(allpairs.toArray(new Tuple2[allpairs.size()])).groupBy(tup2 -> tup2.v1,
						Collectors.mapping(Tuple2::v2, Collectors.toCollection(Vector::new)));
				for (var key : processedgroupbykey.keySet()) {
					var seqtuple2 = Seq.of(processedgroupbykey.get(key).toArray());
					Object foldbykeyresult;
					if (foldbykey.isLeft()) {
						foldbykeyresult = seqtuple2.foldLeft(foldbykey.getValue(), foldbykey.getReduceFunction());
					} else {
						foldbykeyresult = seqtuple2.foldRight(foldbykey.getValue(), foldbykey.getReduceFunction());
					}
					finalfoldbykeyobj.add(Tuple.tuple(key, foldbykeyresult));
				}
				Utils.getKryo().writeClassAndObject(output, finalfoldbykeyobj);
				output.flush();
				var wr = new WeakReference<List>(finalfoldbykeyobj);
				finalfoldbykeyobj = null;

			} else if (!mapgpbykey.isEmpty()) {
				var result = (Map<Object, List<Object>>) mapgpbykey.parallelStream()
						.flatMap(map1 -> map1.entrySet().parallelStream())
						.collect(Collectors.groupingBy((Entry entry) -> entry.getKey(), Collectors
								.mapping((Entry entry) -> entry.getValue(), Collectors.toCollection(Vector::new))));

				var out = result.keySet().parallelStream().map(key -> Tuple.tuple(key, result.get(key)))
						.collect(Collectors.toList());
				Utils.getKryo().writeClassAndObject(output, out);
				output.flush();
				var wr = new WeakReference<List>(out);
				out = null;
			}
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			log.debug("Exiting StreamPipelineTaskExecutor.processFoldByKeyTuple2");
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
	 * 
	 * @return tietaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processCountByKeyTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processCountByKeyTuple2");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = (InputStream) fs;
				var input = new Input(fsdis);
				// while (input.available() > 0) {
				var keyvaluepair = Utils.getKryo().readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedcountbykey = (Map) allpairs.parallelStream()
								.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
						var cf = processedcountbykey.entrySet().stream()
								.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()));
						if (jobstage.getStage().tasks.size() > 1) {
							var functions = getFunctions();
							functions.remove(0);
							cf = (Stream) StreamUtils.getFunctionsToStream(functions, cf.parallel());
						}
						cf.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			var intermediatelist = (List<Tuple2>) null;
			if (!allpairs.isEmpty()) {
				var processedcountbykey = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(Tuple2::v1, (Object v2) -> 1l, (a, b) -> a + b));
				var cf = (CompletableFuture) processedcountbykey.entrySet().stream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				intermediatelist = (List<Tuple2>) cf.get();
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					cf = (CompletableFuture) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.parallelStream()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					intermediatelist = (List<Tuple2>) cf.get();
				}
				Utils.getKryo().writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(intermediatelist);
			intermediatelist = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processCountByKeyTuple2");
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
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processCountByValueTuple2() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processCountByValueTuple2");

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {

			var allpairs = new ArrayList<Tuple2<Object, Object>>();
			for (var fs : task.input) {
				var fsdis = (InputStream) fs;
				var input = new Input(fsdis);
				// while (input.available() > 0) {
				var keyvaluepair = Utils.getKryo().readClassAndObject(input);
				if (keyvaluepair instanceof List kvp) {
					allpairs.addAll(kvp);
				}
				// }
				input.close();
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (!allpairs.isEmpty()) {
						var processedcountbyvalue = (Map) allpairs.parallelStream()
								.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
						var cf = processedcountbyvalue.entrySet().stream()
								.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()));
						if (jobstage.getStage().tasks.size() > 1) {
							var functions = getFunctions();
							functions.remove(0);
							cf = (Stream) StreamUtils.getFunctionsToStream(functions, cf.parallel());
						}
						cf.forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			// Parallel processing of group by key operation.
			var intermediatelist = (List<Tuple2>) null;
			if (!allpairs.isEmpty()) {
				var processedcountbyvalue = (Map) allpairs.parallelStream()
						.collect(Collectors.toMap(tuple2 -> tuple2, (Object v2) -> 1l, (a, b) -> a + b));
				var cf = (CompletableFuture) processedcountbyvalue.entrySet().stream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
								executor, Runtime.getRuntime().availableProcessors()));
				intermediatelist = (List) cf.get();
				if (jobstage.getStage().tasks.size() > 1) {
					var functions = getFunctions();
					functions.remove(0);
					cf = (CompletableFuture) ((Stream) StreamUtils.getFunctionsToStream(functions,
							intermediatelist.stream()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					intermediatelist = (List) cf.get();
				}

				Utils.getKryo().writeClassAndObject(output, intermediatelist);
			}
			output.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(intermediatelist);
			intermediatelist = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processCountByValueTuple2");
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
	 * Result of Coalesce by key operation
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processCoalesce() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processCoalesce");
		var coalescefunction = (List<Coalesce>) getFunctions();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var currentoutput = new Output(sos);) {

			var keyvaluepairs = new ArrayList<Tuple2>();
			for (var fs : task.input) {
				try (var fsis = (InputStream) fs; var input = new Input(fsis);) {
					keyvaluepairs.addAll((List) Utils.getKryo().readClassAndObject(input));
				}
			}
			log.debug("Coalesce Data Size:" + keyvaluepairs.size());
			// Parallel execution of reduce by key stream execution.
			List out = null;
			if (Objects.nonNull(coalescefunction.get(0))
					&& Objects.nonNull(coalescefunction.get(0).getCoalescefunction())) {
				if (coalescefunction.get(0).getCoalescefunction() instanceof PipelineCoalesceFunction pcf) {
					out = Arrays.asList(keyvaluepairs.parallelStream().reduce(pcf).get());
				} else {
					out = keyvaluepairs.parallelStream().collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
							(input1, input2) -> coalescefunction.get(0).getCoalescefunction().apply(input1, input2)))
							.entrySet().stream()
							.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()))
							.get();
				}
			} else {
				out = keyvaluepairs;
			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (out instanceof List) {
						out.parallelStream().forEach(obj -> {
							try {
								if (obj instanceof List objs) {
									objs.stream().forEach(object -> {
										try {
											os.write(object.toString().getBytes());
											os.write(ch);
										} catch (Exception ex) {

										}
									});
								} else {
									os.write(obj.toString().getBytes());
									os.write(ch);
								}
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			var outpairs = out;
			var functions = getFunctions();
			if (functions.size() > 1) {
				functions.remove(0);
				var finaltask = functions.get(functions.size() - 1);
				var stream = StreamUtils.getFunctionsToStream(functions, outpairs.stream());
				if (finaltask instanceof CalculateCount) {
					outpairs = new Vector<>();
					if (stream instanceof IntStream ints) {
						outpairs.add(ints.count());
					} else {
						outpairs.add(((Stream) stream).count());
					}
				} else if (finaltask instanceof PipelineIntStreamCollect piplineistream) {
					outpairs = new Vector<>();
					outpairs.add(((IntStream) stream).collect(piplineistream.getSupplier(),
							piplineistream.getObjIntConsumer(), piplineistream.getBiConsumer()));

				} else {
					CompletableFuture<Vector> cf = (CompletableFuture) ((Stream) stream)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					outpairs = cf.get();
				}
			}
			Utils.getKryo().writeClassAndObject(currentoutput, outpairs);
			currentoutput.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(outpairs);
			outpairs = null;
			log.debug("Exiting StreamPipelineTaskExecutor.processCoalesce");
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
	 * Result of HashPartition operation
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processHashPartition() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processHashPartition");
		var hashpartition = (List<HashPartitioner>) getFunctions();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var currentoutput = new Output(sos);) {

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
				for (int partitionindex = 0;partitionindex < partitionnumber;partitionindex++) {
					output.add(new Tuple2<Integer, List<Tuple2>>(Integer.valueOf(partitionindex),
							mappartitoned.get(partitionindex)));
				}

			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (output instanceof List) {
						output.parallelStream().forEach(obj -> {
							try {
								if (obj instanceof List objs) {
									objs.stream().forEach(object -> {
										try {
											os.write(object.toString().getBytes());
											os.write(ch);
										} catch (Exception ex) {

										}
									});
								} else {
									os.write(obj.toString().getBytes());
									os.write(ch);
								}
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			Utils.getKryo().writeClassAndObject(currentoutput, output);
			currentoutput.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
			var wr = new WeakReference<List>(output);
			output = null;
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
	 * 
	 * @return timetaken in seconds
	 * @throws PipelineException
	 */
	@SuppressWarnings("unchecked")
	public double processGroupBy() throws PipelineException {
		var starttime = System.currentTimeMillis();
		log.debug("Entered StreamPipelineTaskExecutor.processHashPartition");
		var groupbyfunctions = (List<GroupByFunction>) getFunctions();

		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var currentoutput = new Output(sos);) {

			var keyvaluepairs = new ArrayList<>();
			for (var fs : task.input) {
				try (var fsis = (InputStream) fs; var input = new Input(fsis);) {
					keyvaluepairs.addAll((List) Utils.getKryo().readClassAndObject(input));
				}
			}
			log.debug("Data Size:" + keyvaluepairs.size());
			// Parallel execution of reduce by key stream execution.
			List output = null;
			if (Objects.nonNull(groupbyfunctions.get(0))) {
				GroupByFunction gbf = groupbyfunctions.get(0);
				Map<Object, List<Object>> mapgroupby = keyvaluepairs.parallelStream().collect(Collectors.groupingBy(
						obj -> gbf.apply(obj), HashMap::new, Collectors.mapping(obj -> obj, Collectors.toList())));
				output = mapgroupby.keySet().stream()
						.map(key -> new Tuple2<Object, List<Object>>(key, mapgroupby.get(key)))
						.collect(Collectors.toList());

			}
			if (task.finalphase && task.saveresulttohdfs) {
				try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
						Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
								DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
					int ch = (int) '\n';
					if (output instanceof List) {
						output.parallelStream().forEach(obj -> {
							try {
								if (obj instanceof List objs) {
									objs.stream().forEach(object -> {
										try {
											os.write(object.toString().getBytes());
											os.write(ch);
										} catch (Exception ex) {

										}
									});
								} else {
									os.write(obj.toString().getBytes());
									os.write(ch);
								}
							} catch (Exception ex) {

							}
						});
					}
				}
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				return timetaken;
			}
			Utils.getKryo().writeClassAndObject(currentoutput, output);
			currentoutput.flush();
			task.setNumbytesgenerated(fsdos.toByteArray().length);
			cacheAble(fsdos);
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
