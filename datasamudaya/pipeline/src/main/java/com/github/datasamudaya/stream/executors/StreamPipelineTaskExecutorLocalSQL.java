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
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.PipelineConstants;
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
import com.univocity.parsers.common.IterableResult;
import com.univocity.parsers.common.ParsingContext;
import com.univocity.parsers.common.ResultIterator;
import com.univocity.parsers.csv.CsvParser;
import com.univocity.parsers.csv.CsvParserSettings;

import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorLocalSQL extends StreamPipelineTaskExecutorLocal {
	private static Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorLocalSQL.class);

	static ConcurrentMap<BlocksLocation, String> blorcmap = new ConcurrentHashMap<>();

	public StreamPipelineTaskExecutorLocalSQL(JobStage jobstage, ConcurrentMap<String, OutputStream> resultstream,
			Cache cache) {
		super(jobstage, resultstream, cache);
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
		log.info("BlocksLocation Columns: {}", blockslocation.getColumns());
		InputStream istreamnocols = null;
		BufferedReader buffernocols = null;
		YosegiRecordWriter writer = null;
		ByteArrayOutputStream baos = null;
		CsvOptionsSQL csvoptions = (CsvOptionsSQL) jobstage.getStage().tasks.get(0);
		List<String> reqcols = new Vector<>(csvoptions.getRequiredcolumns());
		List<String> originalcolsorder = csvoptions.getRequiredcolumns();
		Collections.sort(reqcols);
		var fsdos = new ByteArrayOutputStream();
		BufferedReader buffer = null;
		InputStream bais = null;
		try (var output = new Output(fsdos);) {
			Stream intermediatestreamobject;
			try {
				byte[] yosegibytes = (byte[]) cache.get(blockslocation.toBlString() + reqcols.toString());
				try {
					if (CollectionUtils.isNotEmpty(csvoptions.getRequiredcolumns())) {
						if (isNull(yosegibytes) || yosegibytes.length == 0 || nonNull(blockslocation.getToreprocess()) && blockslocation.getToreprocess().booleanValue()) {
							log.info("Unable To Find vector for blocks {}", blockslocation);
							bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
							buffer = new BufferedReader(new InputStreamReader(bais));
							task.numbytesprocessed = Utils.numBytesBlocks(blockslocation.getBlock());
							CsvParserSettings settings = new CsvParserSettings();							
							settings.selectIndexes(Utils.indexOfRequiredColumns(originalcolsorder, Arrays.asList(csvoptions.getHeader())));
							settings.getFormat().setLineSeparator("\n");
							settings.setNullValue(DataSamudayaConstants.EMPTY);
							CsvParser parser = new CsvParser(settings);							
							IterableResult<String[], ParsingContext> iter = parser.iterate(buffer);   
							ResultIterator<String[], ParsingContext> iterator = iter.iterator();
					        Spliterator<String[]> spliterator = Spliterators.spliteratorUnknownSize(iterator, Spliterator.SIZED | Spliterator.SUBSIZED);
					        Stream<String[]> stringstream = StreamSupport.stream(spliterator, false);
							Map<String, SqlTypeName> sqltypename = SQLUtils.getColumnTypesByColumn(
									csvoptions.getTypes(), Arrays.asList(csvoptions.getHeader()));
							baos = new ByteArrayOutputStream();
							YosegiRecordWriter writerdataload = writer = new YosegiRecordWriter(baos);
							intermediatestreamobject = stringstream.map(values -> {
								Map data = new LinkedHashMap<>();
								Map datatoprocess = new LinkedHashMap<>();
								try {
									int colcount = 0;
									for(String col:originalcolsorder) {
										SQLUtils.setYosegiObjectByValue(values[colcount], sqltypename.get(col), data,
												col);
										SQLUtils.getValueFromYosegiObject(datatoprocess, col, data);
										colcount++;
									}
									writerdataload.addRow(data);
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
								return datatoprocess;
							});
						} else {
							intermediatestreamobject = SQLUtils.getYosegiStreamRecords(yosegibytes,
									csvoptions.getRequiredcolumns(), Arrays.asList(csvoptions.getHeader()),
									csvoptions.getTypes());
						}
					} else {
						istreamnocols = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
						buffernocols = new BufferedReader(new InputStreamReader(istreamnocols));
						intermediatestreamobject = buffernocols.lines().map(line -> new HashMap<>());
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
					CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					out = cf.get();
					if (task.finalphase && task.saveresulttohdfs) {
						try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
								Short.parseShort(
										DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
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
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
			throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
		} finally {
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

}
