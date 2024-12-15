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
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.stream.BaseStream;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.Command;
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

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import de.siegmar.fastcsv.reader.CommentStrategy;
import de.siegmar.fastcsv.reader.CsvCallbackHandler;
import de.siegmar.fastcsv.reader.CsvReader;
import de.siegmar.fastcsv.reader.NamedCsvRecord;
import de.siegmar.fastcsv.reader.NamedCsvRecordHandler;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

/**
 * Akka actors for the Mapper operators by blocks location
 * @author Arun
 *
 */
public class ProcessMapperByBlocksLocation extends AbstractBehavior<Command> implements Serializable {
	
	Logger log = LoggerFactory.getLogger(ProcessMapperByBlocksLocation.class);
	org.apache.log4j.Logger logger = org.apache.log4j.Logger.getLogger(ProcessMapperByBlocksLocation.class);	

	protected FileSystem hdfs;
	protected boolean completed;
	Cache cache;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	Map<String, Map<RexNode, AtomicBoolean>> blockspartitionfilterskipmap;
	int diskspillpercentage;
	protected List getFunctions(JobStage jobstage) {
		logger.debug("Entered ProcessMapperByBlocksLocation.getFunctions");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		logger.debug("Exiting ProcessMapperByBlocksLocation.getFunctions");
		return functions;
	}
	
	public static EntityTypeKey<Command> createTypeKey(String entityId){ 	
		return EntityTypeKey.create(Command.class, "ProcessMapperByBlocksLocation-"+entityId);
	}
	
	public static Behavior<Command> create(String entityId, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, 
			Map<String, Map<RexNode, AtomicBoolean>> blockspartitionfilterskipmap) {
		return Behaviors.setup(context -> new ProcessMapperByBlocksLocation(context, entityId, hdfs, cache, 
				jobidstageidtaskidcompletedmap, tasktoprocess, blockspartitionfilterskipmap));
	}
	
	private ProcessMapperByBlocksLocation(ActorContext<Command> context,String entityId, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, 
			Map<String, Map<RexNode, AtomicBoolean>> blockspartitionfilterskipmap) {
		super(context);
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.blockspartitionfilterskipmap = blockspartitionfilterskipmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
	}

	@Setter
	@Getter
	@AllArgsConstructor
	@NoArgsConstructor
	public static class BlocksLocationRecord implements Command,Serializable {		
		private static final long serialVersionUID = -4918434381808493015L;
		BlocksLocation bl; 
		FileSystem hdfs;
		Map<Integer, 
		FilePartitionId> filespartitions; 
		List<EntityRef> childactors; 
		Map<Integer, EntityRef> pipeline; 
		JobStage jobstage;
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(BlocksLocationRecord.class, this::processBlocksLocationRecord)
				.build();
	}

	private Behavior<Command> processBlocksLocationRecord(BlocksLocationRecord blr) {
		BlocksLocation blockslocation = blr.bl;
		logger.debug("processing {}"+ blr.bl);
		var starttime = System.currentTimeMillis();
		logger.debug("Entered ProcessMapperByBlocksLocation.processBlocksLocationRecord");
		logger.debug("BlocksLocation Columns: {}"+ blockslocation.getColumns());
		InputStream istreamnocols = null;
		BufferedReader buffernocols = null;
		BufferedReader buffer = null;
		InputStream bais = null;
		List<String> reqcols = null;
		List<String> originalcolsorder = null;
		List<SqlTypeName> sqltypenamel = null;
		final String[] headers;
		boolean iscsv = false;
		final boolean isfilterexist;
		final AtomicBoolean toskipartition;
		RexNode filter;
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
					if (blr.jobstage.getStage().tasks.get(0) instanceof CsvOptionsSQL cosql) {
						reqcols = new Vector<>(cosql.getRequiredcolumns());
						originalcolsorder = new Vector<>(cosql.getRequiredcolumns());
						Collections.sort(reqcols);
						sqltypenamel = cosql.getTypes();
						headers = cosql.getHeader();
						iscsv = true;
						filter = cosql.getFilter();
						String blkey = Utils.getBlocksLocation(blockslocation);
						var toskippartitionoptional = nonNull(filter) &&  nonNull(blockspartitionfilterskipmap.get(blkey))? blockspartitionfilterskipmap
						.get(blkey)
						.entrySet().stream().filter(entry->entry.getKey().equals(filter))
						.map(entry->entry.getValue())
						.findFirst():Optional.empty();
						if(toskippartitionoptional.isPresent()) {
							toskipartition = (AtomicBoolean) toskippartitionoptional.get();
							isfilterexist = true;
						} else if(nonNull(filter)) {
							isfilterexist = false;
							Map<RexNode, AtomicBoolean> filterskipmap = blockspartitionfilterskipmap.get(blkey);
							if(isNull(filterskipmap)) {
								filterskipmap = new ConcurrentHashMap<>();
								blockspartitionfilterskipmap.put(blkey, filterskipmap);
							}
							toskipartition = new AtomicBoolean(true);
							filterskipmap.put(filter, toskipartition);
						} else {
							isfilterexist = false;
							toskipartition = new AtomicBoolean(false);
						}

					} else if (blr.jobstage.getStage().tasks.get(0) instanceof JsonSQL jsql) {
						reqcols = new Vector<>(jsql.getRequiredcolumns());
						originalcolsorder = new Vector<>(jsql.getRequiredcolumns());
						Collections.sort(reqcols);
						sqltypenamel = jsql.getTypes();
						headers = jsql.getHeader();
						iscsv = false;
						toskipartition = new AtomicBoolean(false);
						isfilterexist = true;
						filter = null;
					} else {
						headers = null;
						toskipartition = new AtomicBoolean(false);
						isfilterexist = true;
						filter = null;
					}
					byte[] yosegibytes = new byte[1];
					final List<Integer> oco = originalcolsorder.parallelStream().map(Integer::parseInt).sorted()
							.collect(Collectors.toList());
					if (CollectionUtils.isNotEmpty(originalcolsorder)) {
						if (isNull(yosegibytes) || yosegibytes.length == 1 || nonNull(blockslocation.getToreprocess())
								&& blockslocation.getToreprocess().booleanValue()) {
							logger.debug("Unable To Find vector for blocks {}"+ blockslocation);
							bais = HdfsBlockReader.getBlockDataInputStreamMerge(blockslocation, hdfs);
							buffer = new BufferedReader(new InputStreamReader(bais));
							tasktoprocess.numbytesprocessed = Utils.numBytesBlocks(blockslocation.getBlock());
							Map<String, SqlTypeName> sqltypename = SQLUtils.getColumnTypesByColumn(sqltypenamel,
									Arrays.asList(headers));
							if (iscsv) {
								if (isfilterexist && toskipartition.get()) {
									intermediatestreamobject = Arrays.asList().stream();
								} else {
									CsvCallbackHandler<NamedCsvRecord> callbackHandler = new NamedCsvRecordHandler(
											headers);
									CsvReader<NamedCsvRecord> csv = CsvReader.builder().fieldSeparator(',')
											.quoteCharacter('"').commentStrategy(CommentStrategy.SKIP)
											.commentCharacter('#').skipEmptyLines(true).ignoreDifferentFieldCount(false)
											.detectBomHeader(false).build(callbackHandler, buffer);
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
											logger.error(DataSamudayaConstants.EMPTY, ex);
										}
										if(nonNull(filter) && !isfilterexist && toskipartition.get()) {
											if(SQLUtils.evaluateExpression(filter, valuesobject)) {
												toskipartition.set(false);
											}
										}
										Object[] valueswithconsideration = new Object[2];
										valueswithconsideration[0] = valuesobject;
										valueswithconsideration[1] = toconsidervalueobjects;
										return valueswithconsideration;
									});
								}
							} else {
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
										} catch (Exception ex) {
											logger.error(DataSamudayaConstants.EMPTY, ex);
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
				logger.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				logger.error(PipelineConstants.PROCESSHDFSERROR, ex);
				throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
			}

			intermediatestreamobject.onClose(() -> {
				logger.debug("Stream closed");
			});
			var finaltask = blr.jobstage.getStage().tasks.get(blr.jobstage.getStage().tasks.size() - 1);

			try (var streammap = (BaseStream) StreamUtils.getFunctionsToStream(getFunctions(blr.jobstage),
					intermediatestreamobject);) {
				List out;
				
				int numfileperexec = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC, 
						DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
				logger.debug("Number Of Shuffle Files PerExecutor {}"+ numfileperexec);
				if (MapUtils.isNotEmpty(blr.pipeline)) {
					int totalranges = blr.pipeline.keySet().size();
					logger.debug("Total Ranges {}"+ totalranges);
					ForkJoinPool fjpool = new ForkJoinPool(Runtime.getRuntime().availableProcessors());
					Map<Integer, DiskSpillingList> results = fjpool.submit(()-> (Map) ((Stream<Tuple2>) streammap).collect(
							Collectors.groupingByConcurrent((Tuple2 tup2) -> Math.abs(tup2.v1.hashCode()) % totalranges,
									Collectors.mapping(tup2 -> tup2,
											Collectors.toCollection(() -> new DiskSpillingList(tasktoprocess,
													diskspillpercentage,
													Utils.getUUID().toString(), false, left, right, blr.filespartitions, blr.pipeline, totalranges)))))).get();
					results.entrySet().forEach(entry -> {
						try {
							if(entry.getValue().isSpilled()) {
								entry.getValue().close();
							}
							blr.pipeline.get(entry.getKey()%totalranges).tell(new OutputObject(new ShuffleBlock(null,
											Utils.convertObjectToBytes(blr.filespartitions.get(entry.getKey() % totalranges)), entry.getValue()), left, right, null));
						} catch (Exception e) {
							logger.error(DataSamudayaConstants.EMPTY, e);
						}
					});
					int numexecutorpipe = totalranges/numfileperexec;
					IntStream.range(0, numexecutorpipe).map(val-> val * numfileperexec).forEach(val -> {
						logger.debug("Sending Dummy To Actor: {}"+ blr.pipeline.get(val));
						blr.pipeline.get(val).tell(new OutputObject(new Dummy(), left, right, Dummy.class));
					});
				} else if (CollectionUtils.isNotEmpty(blr.childactors)) {
					logger.debug("Child Actors pipeline Process Started with actors {} with left {} right {} Task {}..."+blr.getChildactors()+ left + right+ getIntermediateDataFSFilePath(tasktoprocess));
					DiskSpillingList diskspilllist = new DiskSpillingList(tasktoprocess,
							diskspillpercentage, DataSamudayaConstants.EMPTY, false, left, right, null, null, 0);
					((Stream) streammap).forEach(diskspilllist::add);
					if(diskspilllist.isSpilled()) {
						diskspilllist.close();
					}
					blr.getChildactors().stream().forEach(
							action -> action.tell(new OutputObject(diskspilllist, left, right, null)));
					blr.getChildactors().stream().forEach(
							action -> action.tell(new OutputObject(new Dummy(), left, right, Dummy.class)));
					logger.debug("Child Actors pipeline Process Ended ...");
				} else {
					logger.debug("Processing Mapper Task In Writing To Cache Started ...");
					DiskSpillingList diskspilllist = new DiskSpillingList(tasktoprocess,
							diskspillpercentage, null, false, left, right, null, null, numfileperexec);
					((Stream) streammap).forEach(diskspilllist::add);
					logger.debug("Processing Mapper Disk Spill Close ...");
					diskspilllist.close();
					logger.debug("Writing To Cache Started with spilled? {}... "+diskspilllist.isSpilled());
					Stream datastream = diskspilllist.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(
							Utils.getLocalFilePathForTask(diskspilllist.getTask(), null, false, false, false)))
							: diskspilllist.readListFromBytes().stream();
					Utils.getKryo().writeClassAndObject(output, datastream.collect(Collectors.toList()));
					output.flush();
					tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
					cacheAble(fsdos);
					diskspilllist.clear();
					logger.debug("Writing To Cache Ended with total bytes {}... " + fsdos.toByteArray().length);
				}
				logger.debug("Map assembly concluded");
				jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);				
				logger.debug("Exiting ProcessMapperByBlocksLocation.processBlocksLocationRecord");
				var timetaken = (System.currentTimeMillis() - starttime) / 1000.0;
				logger.debug("Time taken to compute the Map Task is " + timetaken + " seconds");
				logger.debug("GC Status Map task:" + Utils.getGCStats());
			} catch (IOException ioe) {
				logger.error(PipelineConstants.FILEIOERROR, ioe);
				throw new PipelineException(PipelineConstants.FILEIOERROR, ioe);
			} catch (Exception ex) {
				logger.error(PipelineConstants.PROCESSHDFSERROR, ex);
				throw new PipelineException(PipelineConstants.PROCESSHDFSERROR, ex);
			}
		} catch (Throwable ex) {
			logger.error(PipelineConstants.PROCESSHDFSERROR, ex);
		} finally {			
			if (nonNull(buffer)) {
				try {
					buffer.close();
				} catch (IOException e) {
					logger.error(DataSamudayaConstants.EMPTY, e);
				}
			}
			if (nonNull(bais)) {
				try {
					bais.close();
				} catch (IOException e) {
					logger.error(DataSamudayaConstants.EMPTY, e);
				}
			}
			if (nonNull(buffernocols)) {
				try {
					buffernocols.close();
				} catch (Exception e) {
					logger.error(DataSamudayaConstants.EMPTY, e);
				}
			}
			if (nonNull(istreamnocols)) {
				try {
					istreamnocols.close();
				} catch (Exception e) {
					logger.error(DataSamudayaConstants.EMPTY, e);
				}
			}
			return this;
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
