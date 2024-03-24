package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.HdfsBlockReader;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.CsvOptionsSQL;
import com.github.datasamudaya.stream.JsonSQL;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.stream.utils.StreamUtils;
import com.google.common.collect.Maps;
import com.univocity.parsers.csv.CsvWriter;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvCallbackHandler;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import de.siegmar.fastcsv.reader.NamedCsvRecordHandler;
import jp.co.yahoo.yosegi.config.Configuration;
import jp.co.yahoo.yosegi.writer.YosegiRecordWriter;

/**
 * Akka actors for the Mapper operators by blocks location
 * @author Arun
 *
 */
public class ProcessMapperByBlocksLocation extends AbstractActor implements Serializable {

	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	Cluster cluster = Cluster.get(getContext().getSystem());

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Cache cache;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	private final boolean topersist = false;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	int diskspillpercentage;
	protected List getFunctions() {
		log.debug("Entered ProcessMapperByBlocksLocation.getFunctions");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting ProcessMapperByBlocksLocation.getFunctions");
		return functions;
	}

	private ProcessMapperByBlocksLocation(JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess) {
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
	}

	public static record BlocksLocationRecord(BlocksLocation bl, FileSystem hdfs,
	Map<Integer, FilePartitionId> filespartitions, List<ActorSelection> childactors, Map<Integer, ActorSelection> pipeline) implements Serializable {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(BlocksLocationRecord.class, this::processBlocksLocationRecord)
				.build();
	}

	private void processBlocksLocationRecord(BlocksLocationRecord blr) {
		BlocksLocation blockslocation = blr.bl;
		log.info("processing {}", blr.bl);
		var starttime = System.currentTimeMillis();
		log.info("Entered ProcessMapperByBlocksLocation.processBlocksLocationRecord");
		log.info("BlocksLocation Columns: {}", blockslocation.getColumns());
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
		final boolean left = isNull(tasktoprocess.joinpos) ? false
				: nonNull(tasktoprocess.joinpos) && "left".equals(tasktoprocess.joinpos) ? true : false;
		final boolean right = isNull(tasktoprocess.joinpos) ? false
				: nonNull(tasktoprocess.joinpos) && "right".equals(tasktoprocess.joinpos) ? true : false;
		try (var fsdos = new ByteArrayOutputStream();
				var sos = new SnappyOutputStream(fsdos);
				var output = new Output(sos);) {
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
					byte[] yosegibytes = new byte[1];
					final List<Integer> oco = originalcolsorder.parallelStream().map(Integer::parseInt).sorted()
							.collect(Collectors.toList());
					if (CollectionUtils.isNotEmpty(originalcolsorder)) {
						if (isNull(yosegibytes) || yosegibytes.length == 1 || nonNull(blockslocation.getToreprocess())
								&& blockslocation.getToreprocess().booleanValue()) {
							log.info("Unable To Find vector for blocks {}", blockslocation);
							bais = HdfsBlockReader.getBlockDataInputStreamMerge(blockslocation, hdfs);
							buffer = new BufferedReader(new InputStreamReader(bais));
							tasktoprocess.numbytesprocessed = Utils.numBytesBlocks(blockslocation.getBlock());
							Map<String, SqlTypeName> sqltypename = SQLUtils.getColumnTypesByColumn(sqltypenamel,
									Arrays.asList(headers));
							if (iscsv) {
								CsvCallbackHandler<NamedCsvRecord> callbackHandler = new NamedCsvRecordHandler(
										headers);
								CsvReader<NamedCsvRecord> csv = CsvReader.builder().fieldSeparator(',')
										.quoteCharacter('"').commentStrategy(CommentStrategy.SKIP).commentCharacter('#')
										.skipEmptyLines(true).ignoreDifferentFieldCount(false).detectBomHeader(false)
										.build(callbackHandler, buffer);
								if (topersist) {
									baos = new ByteArrayOutputStream();
								}
								YosegiRecordWriter writerdataload = writer = topersist
										? new YosegiRecordWriter(baos, new Configuration())
										: null;
								intermediatestreamobject = csv.stream().map(values -> {
									Object[] valuesobject = new Object[headers.length];
									Object[] toconsidervalueobjects = new Object[headers.length];
									try {
										for (Integer index : oco) {
											SQLUtils.getValueByIndex(values.getField(index),
													sqltypename.get(headers[index]), valuesobject,
													toconsidervalueobjects, index);
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
								YosegiRecordWriter writerdataload = writer = topersist
										? new YosegiRecordWriter(baos, new Configuration())
										: null;
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
												SQLUtils.setYosegiObjectByValue(reccolval,
														sqltypename.get(headers[index]), data, headers[index]);
												SQLUtils.getValueFromYosegiObject(valuesobject, toconsidervalueobjects,
														headers[index], data, index);
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
							intermediatestreamobject = SQLUtils.getYosegiStreamRecords(yosegibytes, oco,
									Arrays.asList(headers), sqltypenamel);
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

				if (MapUtils.isNotEmpty(blr.pipeline)) {
					int numfilepart = blr.pipeline.keySet().size();
					int numfileperexec = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC, 
							DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
					Map<Integer, DiskSpillingList> results = (Map) ((Stream<Tuple2>) streammap).collect(
							Collectors.groupingByConcurrent((Tuple2 tup2) -> Math.abs(tup2.v1.hashCode()) % numfilepart,
									Collectors.mapping(tup2 -> tup2,
											Collectors.toCollection(() -> new DiskSpillingList(tasktoprocess,
													diskspillpercentage,
													Utils.getUUID().toString(), false, left, right, blr.filespartitions, blr.pipeline, blr.pipeline.size())))));
					results.entrySet().forEach(entry -> {
						try {
							entry.getValue().close();
							blr.pipeline.get(entry.getKey()).tell(new OutputObject(new ShuffleBlock(null,
											Utils.convertObjectToBytes(blr.filespartitions.get(entry.getKey())), entry.getValue()), left, right, Dummy.class),
									ActorRef.noSender());
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					});
					IntStream.range(0, numfilepart).filter(val -> val % numfileperexec == 0).forEach(val -> {
						log.info("Sending Dummy To Actor: {}", blr.pipeline.get(val));
						blr.pipeline.get(val).tell(new OutputObject(new Dummy(), left, right, Dummy.class),
								ActorRef.noSender());
					});
				} else if (CollectionUtils.isNotEmpty(blr.childactors)) {
					DiskSpillingList diskspilllist = new DiskSpillingList(tasktoprocess,
							diskspillpercentage, DataSamudayaConstants.EMPTY, false, left, right, null, null, 0);
					((Stream) streammap).forEach(diskspilllist::add);
					diskspilllist.close();
					blr.childactors().stream().forEach(
							action -> action.tell(new OutputObject(diskspilllist, left, right, Dummy.class), ActorRef.noSender()));
					blr.childactors().stream().forEach(
							action -> action.tell(new OutputObject(new Dummy(), left, right, Dummy.class), ActorRef.noSender()));

				} else {
					DiskSpillingList diskspilllist = new DiskSpillingList(tasktoprocess,
							diskspillpercentage, null, false, left, right, null, null, 0);
					((Stream) streammap).forEach(diskspilllist::add);
					diskspilllist.close();
					Stream datastream = diskspilllist.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(
							Utils.getLocalFilePathForTask(diskspilllist.getTask(), null, false, false, false)))
							: diskspilllist.getData().stream();
					Utils.getKryo().writeClassAndObject(output, datastream.collect(Collectors.toList()));
					output.flush();
					tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
					cacheAble(fsdos);
					diskspilllist.getData().clear();
				}
				jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
				log.info("Map assembly concluded");
				tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
				log.debug("Exiting ProcessMapperByBlocksLocation.processBlocksLocationRecord");
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				log.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
				log.debug("GC Status Map task:" + Utils.getGCStats());
			} catch (IOException ioe) {
				log.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				log.error(PipelineConstants.PROCESSHDFSERROR, ex);
				throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
			}
		} catch (Exception ex) {
			log.error(PipelineConstants.PROCESSHDFSERROR, ex);
		}
	}

	/**
	 * cachable stream
	 * 
	 * @param fsdos
	 */
	@SuppressWarnings("unchecked")
	public void cacheAble(OutputStream fsdos) {
		if (iscacheable) {
			byte[] bt = ((ByteArrayOutputStream) fsdos).toByteArray();
			cache.put(getIntermediateDataFSFilePath(tasktoprocess), bt);
		}
	}

	/**
	 * This method gets the path in jobid-stageid-taskid.
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}
}
