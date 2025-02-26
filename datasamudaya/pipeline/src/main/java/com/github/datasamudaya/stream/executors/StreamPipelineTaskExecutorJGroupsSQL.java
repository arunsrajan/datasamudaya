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
import java.lang.ref.WeakReference;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
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

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jgroups.JChannel;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
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
import com.github.datasamudaya.stream.JsonSQL;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.PipelineIntStreamCollect;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.stream.utils.StreamUtils;
import com.google.common.collect.Maps;
import com.pivovarit.collectors.ParallelCollectors;
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;
import com.univocity.parsers.csv.CsvWriter;
import com.univocity.parsers.csv.CsvWriterSettings;

import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;

/**
 * 
 * @author Arun 
 * Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorJGroupsSQL extends StreamPipelineTaskExecutor {
	private static Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorJGroupsSQL.class);
	private List<Task> tasks;
	Map<String, JobStage> jsidjsmap;
	public double timetaken = 0.0;
	public JChannel channel;
	private int port;
	ConcurrentMap<BlocksLocation, String> blorcmap;
	boolean topersist;

	public StreamPipelineTaskExecutorJGroupsSQL(Map<String, JobStage> jsidjsmap, List<Task> tasks, int port, Cache cache,
			ConcurrentMap<BlocksLocation, String> blorcmap,
			boolean topersist) {
		super(jsidjsmap.get(tasks.get(0).jobid + tasks.get(0).stageid), cache);
		this.jsidjsmap = jsidjsmap;
		this.tasks = tasks;
		this.port = port;
		this.blorcmap = blorcmap;
		this.topersist = topersist;
	}
	ExecutorService es;

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
				log.info("js:{} with id {} with port", jsidjsmap.get(tasktocompute.jobid + tasktocompute.stageid), tasktocompute.jobid + tasktocompute.stageid, port);
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
						InputStream istreamnocols = null;
						BufferedReader buffernocols = null;
						YosegiRecordWriter writer = null;
						ByteArrayOutputStream baos = null;
						BufferedReader buffer = null;
						InputStream bais = null;
						CsvWriter writercsv = null;
						List<String> reqcols = null;
						List<String> originalcolsorder = null;
						List<SqlTypeName> sqltypenamel = null;
						final String[] headers;
						boolean iscsv = false;
						var fsdos = new ByteArrayOutputStream();
						try (var output = new Output(fsdos);) {
							Stream intermediatestreamobject;
							try {
								try {
									if (jobstage.getStage().tasks.get(0) instanceof CsvOptionsSQL cosql) {
										reqcols = new Vector<>(cosql.getRequiredcolumns());
										originalcolsorder = new Vector<>(cosql.getRequiredcolumns());
										Collections.sort(reqcols);
										sqltypenamel = cosql.getTypes();
										headers = cosql.getHeader();
										iscsv = true;

									} else if (jobstage.getStage().tasks.get(0) instanceof JsonSQL jsql) {
										reqcols = new Vector<>(jsql.getRequiredcolumns());
										originalcolsorder = new Vector<>(jsql.getRequiredcolumns());
										Collections.sort(reqcols);
										sqltypenamel = jsql.getTypes();
										headers = jsql.getHeader();
										iscsv = false;
									} else {
										headers = null;
									}
									byte[] yosegibytes = (byte[]) cache.get(blockslocation.toBlString() + reqcols.toString());
									final List<Integer> oco = originalcolsorder.parallelStream().map(Integer::parseInt).sorted().toList();
									if (CollectionUtils.isNotEmpty(originalcolsorder)) {
										if (isNull(yosegibytes) || yosegibytes.length == 0 || nonNull(blockslocation.getToreprocess()) && blockslocation.getToreprocess().booleanValue()) {
											log.info("Unable To Find vector for blocks {}", blockslocation);
											bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
											buffer = new BufferedReader(new InputStreamReader(bais));
											task.numbytesprocessed = Utils.numBytesBlocks(blockslocation.getBlock());
											Map<String, SqlTypeName> sqltypename = SQLUtils.getColumnTypesByColumn(
													sqltypenamel, Arrays.asList(headers));
											if (iscsv) {
												CsvParserSettings settings = new CsvParserSettings();
												settings.getFormat().setLineSeparator("\n");
												settings.selectIndexes(oco.toArray(new Integer[0]));
												settings.setNullValue(DataSamudayaConstants.EMPTY);
												CsvParser parser = new CsvParser(settings);
												IterableResult<String[], ParsingContext> iter = parser.iterate(buffer);
												ResultIterator<String[], ParsingContext> iterator = iter.iterator();
												Spliterator<String[]> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.SIZED | Spliterator.SUBSIZED);
												Stream<String[]> stringstream = StreamSupport.stream(spliterator, false);
												if (topersist) {
													baos = new ByteArrayOutputStream();
												}
												YosegiRecordWriter writerdataload = writer = topersist ? new YosegiRecordWriter(baos) : null;
												intermediatestreamobject = stringstream.map(values -> {
													Map data = Maps.newLinkedHashMap();
													Object[] valuesobject = new Object[headers.length];
													Object[] toconsidervalueobjects = new Object[headers.length];
													int valuesindex = 0;
													try {
														for (String value :values) {
															SQLUtils.setYosegiObjectByValue(value, sqltypename.get(headers[oco.get(valuesindex)]), data,
																	headers[oco.get(valuesindex)]);
															SQLUtils.getValueFromYosegiObject(valuesobject, toconsidervalueobjects, headers[oco.get(valuesindex)], data, oco.get(valuesindex));
															valuesindex++;
														}
														if (topersist) {
															writerdataload.addRow(data);
														}
													} catch (Exception ex) {
														log.error(DataSamudayaConstants.EMPTY, ex);
													}
													Object[] valueswithconsideration = new Object[2];
													valueswithconsideration[0] = valuesobject;
													valueswithconsideration[1] = toconsidervalueobjects;
													return valueswithconsideration;
												});
											} else {
												if (topersist) {
													baos = new ByteArrayOutputStream();
												}
												YosegiRecordWriter writerdataload = writer = topersist ? new YosegiRecordWriter(baos) : null;
												intermediatestreamobject = buffer.lines();
												intermediatestreamobject = intermediatestreamobject.map(line -> {
													try {
														JSONObject jsonobj = (JSONObject) new JSONParser().parse((String) line);
														Map data = Maps.newLinkedHashMap();
														Object[] valuesobject = new Object[headers.length];
														Object[] toconsidervalueobjects = new Object[headers.length];
														try {
															oco.forEach(index -> {
																String reccolval = "";
																if (jsonobj.get(headers[index]) instanceof String val) {
																	reccolval = val;
																} else if (jsonobj.get(headers[index]) instanceof JSONObject jsonval) {
																	reccolval = jsonval.toString();
																} else if (jsonobj.get(headers[index]) instanceof Boolean val) {
																	reccolval = val.toString();
																}
																SQLUtils.setYosegiObjectByValue(reccolval, sqltypename.get(headers[index]), data,
																		headers[index]);
																SQLUtils.getValueFromYosegiObject(valuesobject, toconsidervalueobjects, headers[index], data, index);
															});
															if (topersist) {
																writerdataload.addRow(data);
															}
														} catch (Exception ex) {
															log.error(DataSamudayaConstants.EMPTY, ex);
														}
														Object[] valueswithconsideration = new Object[2];
														valueswithconsideration[0] = valuesobject;
														valueswithconsideration[1] = toconsidervalueobjects;
														return valueswithconsideration;
													} catch (ParseException e) {
														return null;
													}
												});
											}
										} else {
											intermediatestreamobject = SQLUtils.getYosegiStreamRecords(yosegibytes,
													oco, Arrays.asList(headers),
													sqltypenamel);
										}
									} else {
										istreamnocols = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
										buffernocols = new BufferedReader(new InputStreamReader(istreamnocols));
										intermediatestreamobject = buffernocols.lines().map(line -> new Object[1]);
									}
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
									log.info("Map assembly deriving");
									if (task.finalphase && task.saveresulttohdfs) {
										try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
												Short.parseShort(DataSamudayaProperties.get().getProperty(
														DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
														DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));
												) {
											CsvWriterSettings settings = new CsvWriterSettings();
											CsvWriter csvtowrite = writercsv = new CsvWriter(os, settings);
											((Stream) streammap).forEach(value -> {
												try {
													Utils.convertMapToCsv(value, csvtowrite);
												} catch (Exception e) {
													log.error(DataSamudayaConstants.EMPTY, e);
												}
											});
										}
										return (System.currentTimeMillis() - starttime) / 1000.0;
									} else {
										CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
												.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
														executor, Runtime.getRuntime().availableProcessors()));
										out = cf.get();
									}
									log.info("Map assembly concluded");
								}
								Utils.getKryo().writeClassAndObject(output, out);
								output.flush();
								task.setNumbytesgenerated(fsdos.toByteArray().length);
								cacheAble(fsdos);
								var wr = new WeakReference<List>(out);
								out = null;
								log.debug("Exiting StreamPipelineTaskExecutorInMemoryDiskSQL.processBlockHDFSMap");
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
							if (nonNull(writercsv)) {
								try {
									writercsv.close();
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(writer)) {
								try {
									writer.close();
								} catch (IOException e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(baos)) {
								byte[] yosegibytes = baos.toByteArray();
								cache.put(blockslocation.toBlString() + reqcols.toString(), yosegibytes);
								task.numbytesconverted = yosegibytes.length;
								try {
									baos.close();
								} catch (IOException e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(buffer)) {
								try {
									buffer.close();
								} catch (IOException e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(bais)) {
								try {
									bais.close();
								} catch (IOException e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(buffernocols)) {
								try {
									buffernocols.close();
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (nonNull(istreamnocols)) {
								try {
									istreamnocols.close();
								} catch (Exception e) {
									log.error(DataSamudayaConstants.EMPTY, e);
								}
							}
							if (!(task.finalphase && task.saveresulttohdfs)) {
								writeIntermediateDataToDirectByteBuffer(fsdos);
							}
						}

					}


					public Boolean call() {
						hdfs = hdfscompute;
						task = tasktocompute;
						executor = exec;
						log.info("Submitting Task {} in hostport {}", task, task.hostport);
						var stagePartition = task.jobid + task.taskid;
						try {
							var taskspredecessor = task.taskspredecessor;
							log.info("Submitting Task {} in hostport {} with predecessor {}", task, task.hostport, taskspredecessor);
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
									if (breakloop) {
										break;
									}
									Thread.sleep(1000);
								}
							}
							log.debug("Submitted Stage " + stagePartition);

							taskstatusconcmapreq.put(stagePartition, WhoIsResponse.STATUS.RUNNING);
							if (task.input != null && task.parentremotedatafetch != null) {
								var numinputs = task.parentremotedatafetch.length;
								for (var inputindex = 0;inputindex < numinputs;inputindex++) {
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
							log.info("Releasing Semaphore: {} {}", task.hostport, semaphore);
							semaphore.release();
							log.info("Semaphore Released for next task host: {} {}", task.hostport, semaphore);
							cd.countDown();
							log.info("Countdown Released for next task host: {} {}", task.hostport, cd);
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
			if (nonNull(es)) {
				es.shutdown();
				try {
					es.awaitTermination(2, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					log.error("Failed Shutdown executors" + es);
				}
			}
		}
		log.debug("Exiting MassiveDataStreamJGroupsTaskExecutor.call");
		return completed;
	}


}
