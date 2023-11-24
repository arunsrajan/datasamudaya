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
import java.util.List;
import java.util.Objects;
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
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

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

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public final class StreamPipelineTaskExecutorInMemoryDiskSQL extends StreamPipelineTaskExecutorInMemoryDisk {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorInMemoryDiskSQL.class);
	public double timetaken = 0.0;
	public StreamPipelineTaskExecutorInMemoryDiskSQL(JobStage jobstage, ConcurrentMap<String, OutputStream> resultstream,
			Cache cache) throws Exception {
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
		log.debug("Entered StreamPipelineTaskExecutorInMemoryDiskSQL.processBlockHDFSMap");
		log.info("BlocksLocation Columns: {}",blockslocation.getColumns());
		CSVParser records = null;
		InputStream istreamnocols = null;
		BufferedReader buffernocols = null;
		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {
			Stream intermediatestreamobject;
			try {				
				CsvOptionsSQL csvoptions = (CsvOptionsSQL) jobstage.getStage().tasks.get(0);
				List<String> reqcols = new Vector<>(csvoptions.getRequiredcolumns());
				Collections.sort(reqcols);
				byte[] yosegibytes = (byte[]) cache.get(blockslocation.toBlString() + reqcols.toString());
				try {
					if(CollectionUtils.isNotEmpty(csvoptions.getRequiredcolumns())) {
						if(isNull(yosegibytes) || yosegibytes.length==0) {
							log.info("Unable To Find vector for blocks {}",blockslocation);
							try(var bais = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
							var buffer = new BufferedReader(new InputStreamReader(bais));){
								task.numbytesprocessed = Utils.numBytesBlocks(blockslocation.getBlock());
								var csvformat = CSVFormat.DEFAULT.withQuote('"').withEscape('\\');
								csvformat = csvformat.withDelimiter(',').withHeader(csvoptions.getHeader()).withIgnoreHeaderCase()
										.withTrim();
								records = csvformat.parse(buffer);
								Stream<CSVRecord> streamcsv = StreamSupport.stream(records.spliterator(), false);
								yosegibytes = SQLUtils.getYosegiRecordWriter(streamcsv, csvoptions.getTypes(), csvoptions.getRequiredcolumns(), Arrays.asList(csvoptions.getHeader()));
								cache.put(blockslocation.toBlString() + reqcols.toString(), yosegibytes);
							}
						}					 
						intermediatestreamobject = SQLUtils.getYosegiStreamRecords(yosegibytes, csvoptions.getRequiredcolumns(), Arrays.asList(csvoptions.getHeader()), 
								 csvoptions.getTypes());
					} else {
						istreamnocols = HdfsBlockReader.getBlockDataInputStream(blockslocation, hdfs);
						buffernocols = new BufferedReader(new InputStreamReader(istreamnocols));
						intermediatestreamobject = buffernocols.lines().map(line -> new HashMap<>());
					}
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

				} else {
					log.info("Map assembly deriving");
					CompletableFuture<List> cf = (CompletableFuture) ((Stream) streammap)
							.collect(ParallelCollectors.parallel(value -> value, Collectors.toCollection(Vector::new), executor,
									Runtime.getRuntime().availableProcessors()));
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
			if(nonNull(buffernocols)) {
				try {
					buffernocols.close();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
			}
			if(nonNull(istreamnocols)) {
				try {
					istreamnocols.close();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
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
