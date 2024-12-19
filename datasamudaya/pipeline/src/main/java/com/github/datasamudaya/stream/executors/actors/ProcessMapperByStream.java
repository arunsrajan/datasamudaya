package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TerminatingActorValue;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.IteratorType;
import com.github.datasamudaya.common.utils.RemoteIteratorClient;
import com.github.datasamudaya.common.utils.RequestType;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.StreamUtils;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * Akka actors for the Mapper operation by streaming operators
 * 
 * @author Arun
 *
 */
public class ProcessMapperByStream extends AbstractBehavior<Command> implements Serializable {
	private static final Logger logger = LogManager.getLogger(ProcessMapperByStream.class);

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Cache cache;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	private final boolean topersist = false;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<EntityRef> childpipes;
	int terminatingsize;
	int initialsize;
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllist;
	Map<Integer, FilePartitionId> shufflerectowrite;
	Map<Integer, EntityRef> pipeline;
	Map<Integer, EntityRef> actorselections;
	int diskspillpercentage;

	protected List getFunctions() {
		logger.debug("Entered ProcessMapperByStream");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		logger.debug("Exiting ProcessMapperByStream");
		return functions;
	}

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessMapperByStream-" + entityId);
	}

	public static Behavior<Command> create(String entityId, JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<EntityRef> childpipes,
			Map<Integer, FilePartitionId> shufflerectowrite, Map<Integer, EntityRef> pipeline,
			int terminatingsize) {
		return Behaviors.setup(context -> new ProcessMapperByStream(context, js, hdfs,
				cache, jobidstageidtaskidcompletedmap, tasktoprocess, childpipes, shufflerectowrite, pipeline, terminatingsize));
	}

	private ProcessMapperByStream(ActorContext<Command> context, JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<EntityRef> childpipes,
			Map<Integer, FilePartitionId> shufflerectowrite, Map<Integer, EntityRef> pipeline,
			int terminatingsize) {
		super(context);
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.shufflerectowrite = shufflerectowrite;
		this.terminatingsize = terminatingsize;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, diskspillpercentage, null, true, false, false, null,
				null, 1);
		diskspilllist = new DiskSpillingList(tasktoprocess, diskspillpercentage, null, false, false, false, null, null,
				1);
		this.pipeline = pipeline;
		this.actorselections = actorselections;
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(OutputObject.class, this::processMapperByStream).build();
	}

	private Behavior<Command> processMapperByStream(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				if (!dsl.isClosed()) {
					if (dsl.isSpilled()) {
						Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
						dsl.close();
						dsl.clear();
					} else {
						diskspilllistinterm.addAll(dsl.getData());
						dsl.close();
						dsl.clear();
					}
				} else {
					if (dsl.isSpilled()) {
						Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
					} else {
						diskspilllistinterm.addAll(dsl.getData());
					}
				}
				dsl.clear();
			} else if (object.getValue() instanceof DiskSpillingSet dss) {
				if (dss.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dss, diskspilllistinterm);
				} else {
					diskspilllistinterm.addAll(dss.getData());
				}
				dss.clear();
			} else if (object.getValue() instanceof TreeSet treeset) {
				diskspilllistinterm.addAll(treeset);
				treeset.clear();
			}
			if (object.getTerminiatingclass() == DiskSpillingList.class
					|| object.getTerminiatingclass() == Dummy.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				if (diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();
				}
				logger.debug("processMapStream InitialSize {} , Terminating Size {} and Terminating Class {}", initialsize,
						terminatingsize, object.getTerminiatingclass());
				if (object.getTerminiatingclass() == NodeIndexKey.class) {
					logger.debug("Is Terminating Class NodeIndexKey Shuffle Records {} isEmpty {}", shufflerectowrite, MapUtils.isNotEmpty(shufflerectowrite));
					if (CollectionUtils.isNotEmpty(childpipes)) {
						final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "left".equals(tasktoprocess.joinpos) ? true : false;
						final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "right".equals(tasktoprocess.joinpos) ? true
								: false;
						Stream<NodeIndexKey> datastream = diskspilllistinterm.isSpilled()
								? (Stream<NodeIndexKey>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(),
										null, true, false, false)))
								: diskspilllistinterm.getData().stream();
						Map<String, RemoteIteratorClient<NodeIndexKey>> taskrlistiterclientmap = new ConcurrentHashMap<>();
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(),
								datastream.map(nik -> {
									try {
										if (isNull(nik.getTask().getHostport()) || !diskspilllistinterm.isSpilled()) {
											return nik.getValue();
										}
										String jstid = Utils.getIntermediateInputStreamTask(nik.getTask());
										if (isNull(taskrlistiterclientmap.get(jstid))) {
											taskrlistiterclientmap.put(jstid, new RemoteIteratorClient<>(
													nik.getTask(), null, false, false, false, null, RequestType.ELEMENT, IteratorType.SORTORUNIONORINTERSECTION, false, null));
										}
										if (!taskrlistiterclientmap.get(jstid).isclosed() && taskrlistiterclientmap.get(jstid).hasNext()) {
											Object value = taskrlistiterclientmap.get(jstid).next().getValue();
											if (value instanceof NodeIndexKey nikwithvalue) {
												return nikwithvalue.getValue();
											}
											return value;
										}
									} catch (Exception ex) {
										logger.error("{}", ex);
									}
									return null;
								}).filter(Objects::nonNull));) {
							if (MapUtils.isNotEmpty(shufflerectowrite)) {
								int numfilepart = shufflerectowrite.size();
								Map<Integer, DiskSpillingList> results = (Map) ((Stream<Tuple2>) streammap)
										.collect(
												Collectors.groupingByConcurrent(
														(Tuple2 tup2) -> Math.abs(tup2.v1.hashCode()) % numfilepart,
														Collectors.mapping(tup2 -> tup2,
																Collectors.toCollection(() -> new DiskSpillingList(
																		tasktoprocess, diskspillpercentage,
																		Utils.getUUID().toString(), false, leftvalue,
																		rightvalue, null, null, 0)))));
								pipeline.entrySet().stream()
										.forEach(
												entry -> entry.getValue().tell(
														new OutputObject(new TerminatingActorValue(results.size()),
																leftvalue, rightvalue, TerminatingActorValue.class)));
								results.entrySet().forEach(entry -> {
									try {
										if (entry.getValue().isSpilled()) {
											entry.getValue().close();
										}
									} catch (Exception ex) {
										logger.error(DataSamudayaConstants.EMPTY, ex);
									}
									pipeline.get(
											entry.getKey()).tell(
											new OutputObject(
													new ShuffleBlock(null,
															Utils.convertObjectToBytes(
																	shufflerectowrite.get(entry.getKey())),
															entry.getValue()),
													leftvalue, rightvalue, null));
								});
								int numfileperexec = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC,
										DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
								pipeline.keySet().stream().filter(key -> key % numfileperexec == 0)
										.forEach(key -> pipeline.get(key).tell(
												new OutputObject(new Dummy(), leftvalue, rightvalue, Dummy.class)));
							} else {
								streammap.forEach(diskspilllist::add);
								if (diskspilllist.isSpilled()) {
									diskspilllist.close();
								}
								childpipes.stream().forEach(action -> action.tell(
										new OutputObject(diskspilllist, leftvalue, rightvalue, DiskSpillingList.class)));
							}
							taskrlistiterclientmap.values().forEach(client -> {
								try {
									client.close();
								} catch (IOException e) {
									logger.error("{}", e);
								}
							});
						} catch (Exception ex) {
							logger.error("{}", ex);
						}
					} else {
						Stream<NodeIndexKey> datastream = diskspilllistinterm.isSpilled()
								? (Stream<NodeIndexKey>) Utils.getStreamData(new FileInputStream(
								Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true,
										diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
								: diskspilllistinterm.getData().stream();
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream.map(nik -> nik.getValue()));
								var fsdos = new ByteArrayOutputStream();
								var sos = new SnappyOutputStream(fsdos);
								var output = new Output(sos);) {
							logger.debug("Map assembly deriving");
							List result = (List) streammap.collect(Collectors.toList());
							Utils.getKryo().writeClassAndObject(output, result);
							output.flush();
							tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
							cacheAble(fsdos);
							logger.debug("Map assembly concluded");
						} catch (Exception ex) {
							logger.error(DataSamudayaConstants.EMPTY, ex);
						}
					}
				} else {
					logger.debug("Is Terminating Class DiskSpillingList Is Shuffle Records {} Is Empty {} and intermediate Spilled ? {}", shufflerectowrite, MapUtils.isNotEmpty(shufflerectowrite), diskspilllistinterm.isSpilled());
					if (CollectionUtils.isNotEmpty(childpipes)) {
						final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "left".equals(tasktoprocess.joinpos) ? true : false;
						final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "right".equals(tasktoprocess.joinpos) ? true
								: false;
						Stream datastream = diskspilllistinterm.isSpilled()
								? (Stream) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(),
										null, true, false, false)))
								: diskspilllistinterm.getData().stream();
						if (object.getTerminiatingclass() == DiskSpillingSet.class) {
							datastream = ((Stream<NodeIndexKey>) datastream).map(nik -> nik.getValue());
						}
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);) {
							if (MapUtils.isNotEmpty(shufflerectowrite)) {
								int numfilepart = shufflerectowrite.size();
								Map<Integer, DiskSpillingList> results = (Map) ((Stream<Tuple2>) streammap)
										.collect(
												Collectors.groupingByConcurrent(
														(Tuple2 tup2) -> Math.abs(tup2.v1.hashCode()) % numfilepart,
														Collectors.mapping(tup2 -> tup2,
																Collectors.toCollection(() -> new DiskSpillingList(
																		tasktoprocess, diskspillpercentage,
																		Utils.getUUID().toString(), false, leftvalue,
																		rightvalue, null, null, 0)))));
								pipeline.entrySet().stream()
										.forEach(
												entry -> entry.getValue().tell(
														new OutputObject(new TerminatingActorValue(results.size()),
																leftvalue, rightvalue, TerminatingActorValue.class)));
								results.entrySet().forEach(entry -> {
									try {
										if (entry.getValue().isSpilled()) {
											entry.getValue().close();
										}
									} catch (Exception ex) {
										logger.error(DataSamudayaConstants.EMPTY, ex);
									}
									pipeline.get(
											entry.getKey()).tell(
											new OutputObject(
													new ShuffleBlock(null,
															Utils.convertObjectToBytes(
																	shufflerectowrite.get(entry.getKey())),
															entry.getValue()),
													leftvalue, rightvalue, null));
								});
								int numfileperexec = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOTALFILEPARTSPEREXEC,
										DataSamudayaConstants.TOTALFILEPARTSPEREXEC_DEFAULT));
								pipeline.keySet().stream().filter(key -> key % numfileperexec == 0)
										.forEach(key -> pipeline.get(key).tell(
												new OutputObject(new Dummy(), leftvalue, rightvalue, Dummy.class)));
							} else {
								logger.debug("Is Disk Spilling List Spilled {} stream processing for task {}", diskspilllist.isSpilled(), diskspilllist.getTask());
								streammap.forEach(diskspilllist::add);
								logger.debug("Is Disk Spilling List Spilled {} Close for task {}", diskspilllist.isSpilled(), diskspilllist.getTask());
								if (diskspilllist.isSpilled()) {
									diskspilllist.close();
								}
								logger.debug("Is Disk Spilling List Spilled {} for task {}", diskspilllist.isSpilled(), diskspilllist.getTask());
								childpipes.stream().forEach(action -> action.tell(
										new OutputObject(diskspilllist, leftvalue, rightvalue, DiskSpillingList.class)));
							}
						} catch (Exception ex) {
							logger.error(DataSamudayaConstants.EMPTY, ex);
						}
					} else {
						Stream datastream = diskspilllistinterm.isSpilled()
								? (Stream) Utils.getStreamData(new FileInputStream(
								Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true,
										diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
								: diskspilllistinterm.getData().stream();
						if (object.getTerminiatingclass() == DiskSpillingSet.class) {
							datastream = ((Stream<NodeIndexKey>) datastream).map(nik -> nik.getValue());
						}
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);
								var fsdos = new ByteArrayOutputStream();
								var sos = new SnappyOutputStream(fsdos);
								var output = new Output(sos);) {
							logger.debug("Map assembly deriving");
							List result = (List) streammap.collect(Collectors.toList());
							Utils.getKryoInstance().writeClassAndObject(output, result);
							output.flush();
							if(nonNull(fsdos.toByteArray())) {
								logger.info("{}", fsdos.toByteArray().length);
							}
							tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
							cacheAble(fsdos);
							logger.debug("Map assembly concluded");
						} catch (Exception ex) {
							logger.error(DataSamudayaConstants.EMPTY, ex);
						}
					}
				}
				jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
			}
		}
		return this;
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
