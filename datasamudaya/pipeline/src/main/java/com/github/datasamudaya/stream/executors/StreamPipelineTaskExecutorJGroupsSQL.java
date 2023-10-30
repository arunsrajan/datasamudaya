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

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.arrow.vector.ValueVector;
import org.apache.arrow.vector.VectorSchemaRoot;
import org.apache.arrow.vector.ipc.ArrowStreamReader;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVParser;
import org.apache.commons.csv.CSVRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.CompressedVectorSchemaRoot;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
import com.github.datasamudaya.common.WhoIsResponse;
import com.github.datasamudaya.common.functions.CalculateCount;
import com.github.datasamudaya.common.functions.Max;
import com.github.datasamudaya.common.functions.Min;
import com.github.datasamudaya.common.functions.StandardDeviation;
import com.github.datasamudaya.common.functions.Sum;
import com.github.datasamudaya.common.functions.SummaryStatistics;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.CsvOptionsSQL;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.PipelineIntStreamCollect;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.stream.utils.StreamUtils;
import com.pivovarit.collectors.ParallelCollectors;

/**
 * 
 * @author Arun 
 * Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorJGroupsSQL extends StreamPipelineTaskExecutor {
	private static Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorJGroupsSQL.class);
	private List<Task> tasks = null;
	Map<String, JobStage> jsidjsmap;
	public double timetaken = 0.0;
	public JChannel channel;
	private int port;
	ConcurrentMap<BlocksLocation, CompressedVectorSchemaRoot> blvectorsmap;
	public StreamPipelineTaskExecutorJGroupsSQL(Map<String, JobStage> jsidjsmap, List<Task> tasks, int port, Cache cache,
			ConcurrentMap<BlocksLocation, CompressedVectorSchemaRoot> blvectorsmap) {
		super(jsidjsmap.get(tasks.get(0).jobid + tasks.get(0).stageid), cache);
		this.jsidjsmap = jsidjsmap;
		this.tasks = tasks;
		this.port = port;
		this.blvectorsmap = blvectorsmap; 
	}
	ExecutorService es = null;
	
	/**
	 * This method call computes the tasks from stages and return 
	 * whether the tasks are computed successfully.
	 */
	@Override
	public Boolean call() {
		log.debug("Entered MassiveDataStreamJGroupsTaskExecutor.call");
		var taskstatusmap = tasks.parallelStream()
				.map(task -> task.jobid + task.taskid)
				.collect(Collectors.toMap(key -> key, value -> WhoIsResponse.STATUS.YETTOSTART));
		var taskstatusconcmapreq = new ConcurrentHashMap<>(
				taskstatusmap);
		var taskstatusconcmapresp = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL, DataSamudayaConstants.HDFSNAMENODEURL);
		String host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
		Semaphore semaphore = new Semaphore(Runtime.getRuntime().availableProcessors());
		try (var hdfscompute = FileSystem.newInstance(new URI(hdfsfilepath), new Configuration());) {
			this.hdfs = hdfscompute;
			channel = Utils.getChannelTaskExecutor(jobstage.getTejobid(),
					host,
					port, taskstatusconcmapreq, taskstatusconcmapresp);
			log.info("Work in Jgroups agent: " + tasks + " in host: " + host + " port: " + port);
			var cd = new CountDownLatch(tasks.size());
			var exec = executor;
			for (var tasktocompute : tasks) {
				semaphore.acquire();
				log.info("js:{} with id {} with port",jsidjsmap.get(tasktocompute.jobid + tasktocompute.stageid), tasktocompute.jobid + tasktocompute.stageid, port);
				es.submit(new StreamPipelineTaskExecutor(jsidjsmap.get(tasktocompute.jobid + tasktocompute.stageid),
						cache) {
					/**
					 * Perform map operation to obtain intermediate stage result.
					 * 
					 * @param blockslocation
					 * @param hdfs
					 * @return timetaken in seconds
					 * @throws PipelineException
					 */
					@SuppressWarnings("unchecked")
					@Override
					public double processBlockHDFSMap(BlocksLocation blockslocation, FileSystem hdfs)
							throws PipelineException {
						var starttime = System.currentTimeMillis();
						log.debug("Entered StreamPipelineTaskExecutorJGroupsSQL.processBlockHDFSMap");
						log.info("BlocksLocation Columns: {}" + blockslocation.getColumns());
						CSVParser records = null;
						var fsdos = new ByteArrayOutputStream();
						VectorSchemaRoot vectorschemaroot = null;
						ArrowStreamReader readerarrowstream = null;
						List<VectorSchemaRoot> vectorschemaroottoprocess = new ArrayList<>();
						List<ArrowStreamReader> readerarrowstreamtoprocess = new ArrayList<>();
						Map<String, ValueVector> colvalvectormap = null;
						Map<String, VectorSchemaRoot> columnvectorschemaroot = new ConcurrentHashMap<>();
						try (var output = new Output(fsdos);) {
							Stream intermediatestreamobject;
							try {
								CompressedVectorSchemaRoot compvectorschemaroot = blvectorsmap.get(blockslocation);
								Set<String> columsvectorschemaroot = nonNull(compvectorschemaroot)
										? compvectorschemaroot.getColumnvectorschemarootkeymap().keySet()
										: new LinkedHashSet<>();
								Set<String> columsvectorschemaroottoprocess = new LinkedHashSet<>(
										columsvectorschemaroot);
								List<String> columsfromsql = blockslocation.getColumns();
								CsvOptionsSQL csvoptions = (CsvOptionsSQL) jobstage.getStage().tasks.get(0);
								columsvectorschemaroottoprocess.addAll(Arrays.asList(csvoptions.getHeader()));
								columsvectorschemaroottoprocess.addAll(columsfromsql);
								columsvectorschemaroottoprocess.removeAll(columsvectorschemaroot);
								try {
									if (isNull(compvectorschemaroot)
											|| CollectionUtils.isNotEmpty(columsvectorschemaroottoprocess)) {
										log.info("Unable To Find vector for blocks {}", blockslocation);
										try(var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
												var buffer = new BufferedReader(new InputStreamReader(bais));
														var baisrec = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
												var bufferrec = new BufferedReader(new InputStreamReader(baisrec));) {
											var csvformat = CSVFormat.DEFAULT.withQuote('"').withEscape('\\');
											csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader())
													.withIgnoreHeaderCase().withTrim();
											records = csvformat.parse(buffer);
											Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(),
													false);
											int reccount = Long.valueOf(streamcsv.count()).intValue();
											log.info("Records to process {}", reccount);
											streamcsv = StreamSupport.stream(csvformat.parse(bufferrec).spliterator(),
													false);
											CompressedVectorSchemaRoot compressedvectorschemaroot = nonNull(
													compvectorschemaroot) ? compvectorschemaroot
															: new CompressedVectorSchemaRoot();
											Map<String, Integer> columnindexmap = SQLUtils
													.getColumnIndexMap(Arrays.asList(csvoptions.getHeader()));
											VectorSchemaRoot root = SQLUtils.getArrowVectors(streamcsv,
													new ArrayList<>(columsvectorschemaroottoprocess), columnindexmap,
													csvoptions.getTypes(), compressedvectorschemaroot, reccount);
											blvectorsmap.put(blockslocation, compressedvectorschemaroot);
											compvectorschemaroot = compressedvectorschemaroot;
											for (String columnsql : columsfromsql) {
												columnvectorschemaroot.put(columnsql, root);
											}
										}
									} else {
										compvectorschemaroot = blvectorsmap.get(blockslocation);
										Map<String, String> columnvectorschemarootkeymap = compvectorschemaroot
												.getColumnvectorschemarootkeymap();
										Map<String, String> vectorschemarootkeyfilemap = compvectorschemaroot
												.getVectorschemarootkeybytesmap();
										List<String> processedkeys = new ArrayList<>();
										Map<String, VectorSchemaRoot> keyvectorschemaroot = new ConcurrentHashMap<>();

										for (String columnsql : columsfromsql) {
											String vectorschemarootkey = columnvectorschemarootkeymap.get(columnsql);
											if (!processedkeys.contains(vectorschemarootkey)) {
												readerarrowstream = SQLUtils.decompressVectorSchemaRootBytes(
														vectorschemarootkeyfilemap.get(vectorschemarootkey));
												readerarrowstreamtoprocess.add(readerarrowstream);
												vectorschemaroot = readerarrowstream.getVectorSchemaRoot();
												vectorschemaroottoprocess.add(vectorschemaroot);
												keyvectorschemaroot.put(vectorschemarootkey, vectorschemaroot);
												processedkeys.add(vectorschemarootkey);
											}
											columnvectorschemaroot.put(columnsql,
													keyvectorschemaroot.get(vectorschemarootkey));
										}
									}
									final int totalrecords = compvectorschemaroot.getRecordcount();
									List<String> columntoquery = blockslocation.getColumns();
									Map<String, ValueVector> colvalvectormapl = columntoquery.stream()
											.collect(Collectors.toMap(val -> val, val -> {
												ValueVector valuevector = columnvectorschemaroot.get(val)
														.getVector((String) val);
												return valuevector;
											}));
									colvalvectormap = colvalvectormapl;
									log.info("Processing Data for blockslocation {}", blockslocation);
									intermediatestreamobject = IntStream.range(0, totalrecords)
											.mapToObj(recordIndex -> {
												Map<String, Object> valuemap = new ConcurrentHashMap<>();
												columntoquery.stream().forEach(column -> {
													Object arrowvectorvalue = colvalvectormapl.get(column);
													valuemap.put(column,
															SQLUtils.getVectorValue(recordIndex, arrowvectorvalue));
												});
												return valuemap;
											});
								} finally {
								}
							} catch (IOException ioe) {
								log.error(PipelineConstants.FILEIOERROR, ioe);
								throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
							} catch (Exception ex) {
								log.error(PipelineConstants.PROCESSHDFSERROR, ex);
								throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
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
									CompletableFuture<List> cf = (CompletableFuture) ((java.util.stream.IntStream) streammap)
											.boxed()
											.collect(ParallelCollectors.parallel(value -> value,
													Collectors.toCollection(Vector::new), executor,
													Runtime.getRuntime().availableProcessors()));
									var streamtmp = cf.get();
									var mean = (streamtmp).stream().mapToInt(Integer.class::cast).average()
											.getAsDouble();
									var variance = (streamtmp).stream().mapToInt(Integer.class::cast)
											.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
									var standardDeviation = Math.sqrt(variance);
									out.add(standardDeviation);

								} else {
									log.info("Map assembly deriving");
									CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
											.collect(ParallelCollectors.parallel(value -> value,
													Collectors.toCollection(Vector::new), executor,
													Runtime.getRuntime().availableProcessors()));
									out = cf.get();
									if (task.finalphase && task.saveresulttohdfs) {
										try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
												Short.parseShort(DataSamudayaProperties.get().getProperty(
														DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
														DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
											Utils.convertToCsv((List) out, os);
										}
										var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
										return timetaken;
									}
									log.info("Map assembly concluded");
								}
								Utils.getKryo().writeClassAndObject(output, out);
								output.flush();
								cacheAble(fsdos);
								log.debug("Exiting StreamPipelineTaskExecutorJGroupsSQL.processBlockHDFSMap");
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
						} catch (Exception ex) {
							log.error(PipelineConstants.PROCESSHDFSERROR, ex);
							throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
						} finally {
							vectorschemaroottoprocess.stream().forEach(vsr -> {
								if (nonNull(vsr)) {
									vsr.clear();
									vsr.close();
								}
							});
							if (Objects.nonNull(colvalvectormap)) {
								for (String key : new HashSet<>(colvalvectormap.keySet())) {
									colvalvectormap.get(key).clear();
									colvalvectormap.get(key).close();
									colvalvectormap.remove(key);
								}
							}
							readerarrowstreamtoprocess.stream().forEach(reader -> {
								if (nonNull(reader)) {
									try {
										reader.close();
									} catch (IOException e) {
										log.error(DataSamudayaConstants.EMPTY, e);
									}
								}
							});
							if (!(task.finalphase && task.saveresulttohdfs)) {
								writeIntermediateDataToDirectByteBuffer(fsdos);
							}
							if (!Objects.isNull(records)) {
								try {
									records.close();
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
						}

					}
					
					
					public Boolean call() {
						hdfs = hdfscompute;
						task = tasktocompute;
						executor = exec;
						log.info("Submitting Task {} in hostport {}",task, task.hostport);
						var stagePartition = task.jobid + task.taskid;
						try {
							var taskspredecessor = task.taskspredecessor;
							log.info("Submitting Task {} in hostport {} with predecessor {}",task, task.hostport, taskspredecessor);
							if (!taskspredecessor.isEmpty()) {
								var taskids = taskspredecessor.parallelStream().map(tk -> tk.jobid + tk.taskid)
										.collect(Collectors.toList());
								var breakloop = false;
								while (true) {
									var tasktatusconcmap = new ConcurrentHashMap<String, WhoIsResponse.STATUS>();
									tasktatusconcmap.putAll(taskstatusconcmapresp);
									tasktatusconcmap.putAll(taskstatusconcmapreq);
									breakloop = true;
									for (var taskid : taskids) {
										if (taskstatusconcmapresp.get(taskid) != null && taskstatusconcmapresp
												.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
											Utils.whois(channel, taskid);
											breakloop = false;
											continue;
										} else if (tasktatusconcmap.get(taskid) != null) {
											if (tasktatusconcmap.get(taskid) != WhoIsResponse.STATUS.COMPLETED) {
												breakloop = false;
												continue;
											}

										} else {
											Utils.whois(channel, taskid);
											breakloop = false;
											continue;
										}
									}
									if (breakloop)
										break;
									Thread.sleep(1000);
								}
							}
							log.debug("Submitted Stage " + stagePartition);

							taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.RUNNING);
							if (task.input != null && task.parentremotedatafetch != null) {
								var numinputs = task.parentremotedatafetch.length;
								for (var inputindex = 0; inputindex < numinputs; inputindex++) {
									var input = task.parentremotedatafetch[inputindex];
									if (input != null) {
										var rdf = input;
										InputStream is = RemoteDataFetcher.readIntermediatePhaseOutputFromFS(
												rdf.getJobid(), getIntermediateDataRDF(rdf.getTaskid()));
										if (Objects.isNull(is)) {
											RemoteDataFetcher.remoteInMemoryDataFetch(rdf);
											task.input[inputindex] = new ByteArrayInputStream(rdf.getData());
										} else {
											task.input[inputindex] = is;
										}
									}
								}
							}
							jobstage = jsidjsmap.get(tasktocompute.jobid + tasktocompute.stageid);
							var timetakenseconds = computeTasks(task, hdfs);
							log.info("Completed Stage " + stagePartition + " in " + timetakenseconds);
							taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.COMPLETED);
						} catch (Exception ex) {
							log.error("Failed Stage " + tasks, ex);
							completed = false;
						} finally {
							log.info("Releasing Semaphore: {} {}",task.hostport, semaphore ); 
							semaphore.release();
							log.info("Semaphore Released for next task host: {} {}",task.hostport, semaphore);
							cd.countDown();
							log.info("Countdown Released for next task host: {} {}",task.hostport, cd);
						}
						return completed;
					}
				});
			}
			log.info("StagePartitionId with Stage Statuses: " + taskstatusconcmapreq
					+ " WhoIs Response stauses: " + taskstatusconcmapresp);
			cd.await();
			completed = true;			
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
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
			if(nonNull(es)) {
				es.shutdown();
				try {
					es.awaitTermination(2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("Failed Shutdown executors"+ es);
				}
			}
		}
		log.debug("Exiting MassiveDataStreamJGroupsTaskExecutor.call");
		return completed;
	}
	
	

}
