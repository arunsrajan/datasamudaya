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
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.lang.ref.WeakReference;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.CompressedVectorSchemaRoot;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
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
import com.google.common.collect.MapMaker;
import com.pivovarit.collectors.ParallelCollectors;

/**
 * 
 * @author Arun
 * Task executors thread for standalone task executors daemon.  
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorLocalSQL extends StreamPipelineTaskExecutorLocal  {
	private static Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorLocalSQL.class);

	static ConcurrentMap<BlocksLocation, CompressedVectorSchemaRoot> blvectorsmap = new ConcurrentHashMap<>();
	
	public StreamPipelineTaskExecutorLocalSQL(JobStage jobstage,
			ConcurrentMap<String, OutputStream> resultstream, Cache cache) {
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
		log.info("BlocksLocation Columns: {}"+blockslocation.getColumns());
		CSVParser records = null;
		var fsdos = new ByteArrayOutputStream();
		VectorSchemaRoot vectorschemaroot = null;
		List<VectorSchemaRoot> vectorschemaroottoprocess = new ArrayList<>();
		Map<String, ValueVector> colvalvectormap = null;
		Map<String, VectorSchemaRoot> columnvectorschemaroot = new ConcurrentHashMap<>();
		try (var output = new Output(fsdos);) {
			Stream intermediatestreamobject;
			try {
				CompressedVectorSchemaRoot compvectorschemaroot = blvectorsmap.get(blockslocation);
				Set<String> columsvectorschemaroot = nonNull(compvectorschemaroot)? compvectorschemaroot.getColumnvectorschemarootkeymap().keySet():
					new LinkedHashSet<>();
				Set<String> columsvectorschemaroottoprocess = new LinkedHashSet<>(columsvectorschemaroot);
				List<String> columsfromsql = blockslocation.getColumns();
				CsvOptionsSQL csvoptions = (CsvOptionsSQL) jobstage.getStage().tasks.get(0);
				columsvectorschemaroottoprocess.addAll(Arrays.asList(csvoptions.getHeader()));
				columsvectorschemaroottoprocess.addAll(columsfromsql);
				columsvectorschemaroottoprocess.removeAll(columsvectorschemaroot);
				try {
					if(isNull(compvectorschemaroot) || CollectionUtils.isNotEmpty(columsvectorschemaroottoprocess)) {
						log.info("Unable To Find vector for blocks {}",blockslocation);
						try(var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
						var buffer = new BufferedReader(new InputStreamReader(bais));
								var baisrec = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
								var bufferrec = new BufferedReader(new InputStreamReader(baisrec));){						
							var csvformat = CSVFormat.DEFAULT.withQuote('"').withEscape('\\');
							csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader()).withIgnoreHeaderCase()
									.withTrim();
							records = csvformat.parse(buffer);
							Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(), false);
							int reccount = Long.valueOf(streamcsv.count()).intValue();
							log.info("Records to process {}", reccount);
							streamcsv = StreamSupport.stream(csvformat.parse(bufferrec).spliterator(), false);
							CompressedVectorSchemaRoot compressedvectorschemaroot = nonNull(compvectorschemaroot)?
									compvectorschemaroot:new CompressedVectorSchemaRoot();
							Map<String, Integer> columnindexmap = SQLUtils.getColumnIndexMap(Arrays.asList(csvoptions.getHeader()));
							VectorSchemaRoot root = SQLUtils.getArrowVectors(streamcsv,new ArrayList<>(columsvectorschemaroottoprocess), columnindexmap, csvoptions.getTypes(), compressedvectorschemaroot, reccount);
							blvectorsmap.put(blockslocation, compressedvectorschemaroot);
							compvectorschemaroot = compressedvectorschemaroot;
							for(String columnsql:columsfromsql) {
								columnvectorschemaroot.put(columnsql, root);
							}
						}
					} else {
						compvectorschemaroot = blvectorsmap.get(blockslocation);
						Map<String, String> columnvectorschemarootkeymap = compvectorschemaroot.getColumnvectorschemarootkeymap();
						Map<String, String> vectorschemarootkeyfilemap = compvectorschemaroot.getVectorschemarootkeybytesmap();
						List<String> processedkeys = new ArrayList<>();
						Map<String, VectorSchemaRoot> keyvectorschemaroot = new ConcurrentHashMap<>();						
						for(String columnsql:columsfromsql) {
							String vectorschemarootkey = columnvectorschemarootkeymap.get(columnsql);
							if(!processedkeys.contains(vectorschemarootkey)) {
								vectorschemaroot = SQLUtils.decompressVectorSchemaRootBytes(vectorschemarootkeyfilemap.get(vectorschemarootkey));;
								vectorschemaroottoprocess.add(vectorschemaroot);
								keyvectorschemaroot.put(vectorschemarootkey, vectorschemaroot);
								processedkeys.add(vectorschemarootkey);
							} 
							columnvectorschemaroot.put(columnsql, keyvectorschemaroot.get(vectorschemarootkey));
						}
					}
					final int totalrecords = compvectorschemaroot.getRecordcount();
					List<String> columntoquery = blockslocation.getColumns();
					Map<String, ValueVector> colvalvectormapl = columntoquery.stream().collect(Collectors.toMap(val->val, val->{
						ValueVector valuevector = columnvectorschemaroot.get(val).getVector((String) val);
						return valuevector;
					}));
					colvalvectormap = colvalvectormapl;
					log.info("Processing Data for blockslocation {}",blockslocation);
					intermediatestreamobject = IntStream.range(0, totalrecords).boxed().map(recordIndex -> {
						List<String> columntoqueryl = columntoquery;
						Map<String, Object> valuemap = new ConcurrentHashMap<>();
						columntoqueryl.stream().forEach(column->{
							Object arrowvectorvalue = colvalvectormapl.get(column);
							valuemap.put(column, SQLUtils.getVectorValue(recordIndex, arrowvectorvalue));
						});
						return valuemap;
					});
				} finally {}
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
					CompletableFuture<List> cf = (CompletableFuture) ((java.util.stream.IntStream) streammap).boxed()
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					var streamtmp = cf.get();
					var mean = (streamtmp).stream().mapToInt(Integer.class::cast).average().getAsDouble();
					var variance = (streamtmp).stream().mapToInt(Integer.class::cast)
							.mapToDouble(i -> (i - mean) * (i - mean)).average().getAsDouble();
					var standardDeviation = Math.sqrt(variance);
					out.add(standardDeviation);

				} else if (task.finalphase && task.saveresulttohdfs) {
					try (OutputStream os = hdfs.create(new Path(task.hdfsurl + task.filepath),
							Short.parseShort(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DFSOUTPUTFILEREPLICATION,
									DataSamudayaConstants.DFSOUTPUTFILEREPLICATION_DEFAULT)));) {
						int ch = (int) '\n';
						((Stream) streammap).forEach(val -> {
							try {
								os.write(val.toString().getBytes());
								os.write(ch);
							} catch (IOException e) {
							}
						});
					}
					var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
					return timetaken;
				} else {
					log.info("Map assembly deriving");
					CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new),
									executor, Runtime.getRuntime().availableProcessors()));
					out = cf.get();
					log.info("Map assembly concluded");
				}
				Utils.getKryo().writeClassAndObject(output, out);
				output.flush();
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
			vectorschemaroottoprocess.stream().forEach(vsr -> {
				if (nonNull(vsr)) {
					vsr.clear();
					vsr.close();
				}
			});
			if (Objects.nonNull(colvalvectormap)) {
				for(String key:new HashSet<>(colvalvectormap.keySet())) {
					colvalvectormap.get(key).clear();
					colvalvectormap.get(key).close();
					colvalvectormap.remove(key);
				}
			}
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
	
}
