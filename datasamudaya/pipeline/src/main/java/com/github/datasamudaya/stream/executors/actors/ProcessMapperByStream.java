package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TerminatingActorValue;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.StreamUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;

/**
 * Akka actors for the Mapper operation by streaming operators
 * @author Arun
 *
 */
public class ProcessMapperByStream extends AbstractActor implements Serializable {
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
	List<ActorSelection> childpipes;
	int terminatingsize;
	int initialsize;
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllist;
	Map<Integer, FilePartitionId> shufflerectowrite;
	Map<Integer, ActorSelection> pipeline;
	Map<Integer, ActorSelection> actorselections;

	protected List getFunctions() {
		log.debug("Entered ProcessMapperByStream");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting ProcessMapperByStream");
		return functions;
	}

	private ProcessMapperByStream(JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<ActorSelection> childpipes,
			Map<Integer, FilePartitionId> shufflerectowrite,
			Map<Integer, ActorSelection> pipeline,
			int terminatingsize) {
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.shufflerectowrite = shufflerectowrite;
		this.terminatingsize = terminatingsize;
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, true, false, false, null, null, 0);
		diskspilllist = new DiskSpillingList(tasktoprocess, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, false, false, false, null, null, 0);
		this.pipeline = pipeline;
		this.actorselections = actorselections;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processMapperByStream)
				.build();
	}

	private void processMapperByStream(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				if (dsl.isSpilled()) {
					log.info("In Mapper Stream Spilled {}", dsl.isSpilled());
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
				} else {					
					diskspilllistinterm.addAll(dsl.readListFromBytes());
				}
			}
			if(object.getTerminiatingclass() == DiskSpillingList.class) {
				initialsize++;
			} else if(object.getTerminiatingclass() == Dummy.class) {
				initialsize++;
			} else if(object.getTerminiatingclass() == NodeIndexKey.class) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				diskspilllistinterm.close();
				log.info("processMapStream::Started InitialSize {} , Terminating Size {}", initialsize,
						terminatingsize);
				if (object.getTerminiatingclass() == NodeIndexKey.class) {
					try (var baos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(baos);
							var output = new Output(sos);) {
						Utils.getKryo().writeClassAndObject(output, object.getValue());
						output.flush();
						cache.put(getIntermediateDataFSFilePath(tasktoprocess), baos.toByteArray());
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
				}
				else {
					Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(
									new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null,
											true, diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
							: diskspilllistinterm.readListFromBytes().stream();
					try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);
							var fsdos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(fsdos);
							var output = new Output(sos);) {
						log.info("Map assembly deriving");
						List result = (List) streammap.collect(Collectors.toList());
						Utils.getKryo().writeClassAndObject(output, result);
						output.flush();
						tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
						cacheAble(fsdos);
						log.info("Map assembly concluded");
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					if (CollectionUtils.isNotEmpty(childpipes)) {
						final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "left".equals(tasktoprocess.joinpos) ? true : false;
						final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
								: nonNull(tasktoprocess.joinpos) && "right".equals(tasktoprocess.joinpos) ? true : false;
						datastream = diskspilllistinterm.isSpilled()
								? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils
										.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true, false, false)))
								: diskspilllistinterm.readListFromBytes().stream();
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);) {
							if (MapUtils.isNotEmpty(shufflerectowrite)) {
								int numfilepart = shufflerectowrite.size();
								Map<Integer, DiskSpillingList> results = (Map) ((Stream<Tuple2>) streammap)
										.collect(Collectors.groupingByConcurrent(
												(Tuple2 tup2) -> Math.abs(tup2.v1.hashCode()) % numfilepart,
												Collectors.mapping(tup2 -> tup2,
														Collectors.toCollection(() -> new DiskSpillingList(tasktoprocess,
																DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
																Utils.getUUID().toString(), false, leftvalue, rightvalue,
																null, null, 0)))));
								pipeline.entrySet().stream().forEach(entry -> entry.getValue().tell(
										new OutputObject(new TerminatingActorValue(results.size()), leftvalue, rightvalue, Dummy.class),
										ActorRef.noSender()));
								results.entrySet().forEach(entry -> {
									try {
										entry.getValue().close();
									} catch (Exception ex) {
										log.error(DataSamudayaConstants.EMPTY, ex);
									}
									pipeline.get(entry.getKey())
											.tell(new OutputObject(new ShuffleBlock(null,
													Utils.convertObjectToBytes(shufflerectowrite.get(entry.getKey())),
													entry.getValue()), leftvalue, rightvalue, Dummy.class), ActorRef.noSender());
								});
								pipeline.entrySet().stream().forEach(entry -> entry.getValue()
										.tell(new OutputObject(new Dummy(), leftvalue, rightvalue, Dummy.class), ActorRef.noSender()));
							} else {
								streammap.forEach(diskspilllist::add);
								diskspilllist.close();
								childpipes.stream().forEach(action -> action
										.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, DiskSpillingList.class), ActorRef.noSender()));
							}
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}					
					}
				}
				jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
			}
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
