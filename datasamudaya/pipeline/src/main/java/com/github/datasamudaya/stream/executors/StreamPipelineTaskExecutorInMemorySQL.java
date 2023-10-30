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
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.HashSet;
import java.util.ArrayList;
import java.util.List;
import java.util.LinkedHashSet;
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
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.ehcache.Cache;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferInputStream;
import com.github.datasamudaya.common.ByteBufferOutputStream;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CompressedVectorSchemaRoot;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.RemoteDataFetch;
import com.github.datasamudaya.common.RemoteDataFetcher;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskStatus;
import com.github.datasamudaya.common.TaskType;
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

import gnu.trove.map.TMap;

/**
 * 
 * @author Arun Task executors thread for standalone task executors daemon.
 */
@SuppressWarnings("rawtypes")
public class StreamPipelineTaskExecutorInMemorySQL extends StreamPipelineTaskExecutor {
	private static org.slf4j.Logger log = LoggerFactory.getLogger(StreamPipelineTaskExecutorInMemorySQL.class);
	protected ConcurrentMap<String, OutputStream> resultstream = null;
	public double timetaken = 0.0;
	private ConcurrentMap<BlocksLocation, CompressedVectorSchemaRoot> blvectorsmap;

	public StreamPipelineTaskExecutorInMemorySQL(JobStage jobstage, ConcurrentMap<String, OutputStream> resultstream,
			Cache cache,ConcurrentMap<BlocksLocation, CompressedVectorSchemaRoot> blvectorsmap) {
		super(jobstage, cache);
		this.resultstream = resultstream;
		this.blvectorsmap = blvectorsmap;
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
		ArrowStreamReader readerarrowstream = null;
		List<VectorSchemaRoot> vectorschemaroottoprocess = new ArrayList<>();
		List<ArrowStreamReader> readerarrowstreamtoprocess = new ArrayList<>();
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
								readerarrowstream = SQLUtils.decompressVectorSchemaRootBytes(vectorschemarootkeyfilemap.get(vectorschemarootkey));
								readerarrowstreamtoprocess.add(readerarrowstream);
								vectorschemaroot = readerarrowstream.getVectorSchemaRoot();
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
						Map<String, Object> valuemap = new MapMaker()
							    .concurrencyLevel(4) // Adjust as needed
							    .initialCapacity(16) // Adjust as needed
							    .weakKeys()
							    .makeMap();
						columntoqueryl.parallelStream().forEach(column->{
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
	
	/**
	 * The path from RDF to output stream.
	 * 
	 * @param rdf
	 * @return output stream
	 * @throws Exception
	 */
	public OutputStream getIntermediateInputStreamRDF(RemoteDataFetch rdf) throws Exception {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamRDF");
		var path = (rdf.getJobid() + DataSamudayaConstants.HYPHEN + rdf.getStageid() + DataSamudayaConstants.HYPHEN + rdf.getTaskid());
		OutputStream os = resultstream.get(path);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
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
	 * 
	 * @return jobid-stageid-taskid
	 */
	@Override
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}

	/**
	 * Create a new BytebufferOutputStream using task and buffer size and return it.
	 */
	@Override
	public OutputStream createIntermediateDataToFS(Task task, int buffersize) throws PipelineException {
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
		try {
			var path = getIntermediateDataFSFilePath(task);
			OutputStream os;
			os = new ByteBufferOutputStream(ByteBufferPoolDirect.get(buffersize));
			resultstream.put(path, os);
			log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.createIntermediateDataToFS");
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
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		var path = getIntermediateDataFSFilePath(task);
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.getIntermediateInputStreamFS");
		OutputStream os = resultstream.get(path);
		if (os instanceof ByteBufferOutputStream baos) {
			return new ByteBufferInputStream(baos.get());
		} else {
			throw new UnsupportedOperationException("Unknown I/O operation");
		}

	}

	/**
	 * This method call computes the tasks from stages and return whether the tasks
	 * are computed successfully.
	 */
	@Override
	public Boolean call() {
		starttime = System.currentTimeMillis();
		log.debug("Entered MassiveDataStreamTaskExecutorInMemory.call for task " + task);
		String stageTasks = "";
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		log.info("Acclaimed namenode URL " + hdfsfilepath);
		var configuration = new Configuration();
		try (var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);) {
			stageTasks = getStagesTask();
			log.info("Submitted Task " + task);
			if (task.input != null && task.parentremotedatafetch != null) {
				var numinputs = task.parentremotedatafetch.length;
				for (var inputindex = 0; inputindex < numinputs; inputindex++) {
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
			}
			log.info("Functioning Task " + task);
			timetaken = computeTasks(task, hdfs);
			log.info("Completed Task: " + task);
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
		log.debug("Exiting MassiveDataStreamTaskExecutorInMemory.call");
		return completed;
	}

}
