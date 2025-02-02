package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
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
 * Akka actors for the Reduce operators.
 * @author Arun
 *
 */
public class ProcessReduce extends AbstractBehavior<Command> implements Serializable {
	Logger log = LogManager.getLogger(ProcessReduce.class);

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
	int dummysize;
	int terminatingsize;
	int initialshufflesize;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistinterm;
	int diskspillpercentage;
	ForkJoinPool fjpool;
	protected List getFunctions() {
		log.debug("Entered ProcessReduce");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting ProcessReduce");
		return functions;
	}

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessReduce-" + entityId);
	}

	public static Behavior<Command> create(String entityId, JobStage js, FileSystem hdfs, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<EntityRef> childpipes, int terminatingsize, ForkJoinPool fjpool) {
		return Behaviors.setup(context -> new ProcessReduce(context, js, hdfs, cache,
				jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, terminatingsize, fjpool));
	}

	private ProcessReduce(ActorContext<Command> context, JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<EntityRef> childpipes,
			int terminatingsize, ForkJoinPool fjpool) {
		super(context);
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.terminatingsize = terminatingsize;
		this.fjpool = fjpool;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, diskspillpercentage,
				DataSamudayaConstants.EMPTY, true, false, false, null, null, 0);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processReduce)
				.onMessage(EntityRefStop.class, this::behaviorStop)
				.build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}
	
	private Behavior<Command> processReduce(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof ShuffleBlock sb) {
				try {
					Object obj = sb.getData();
					CompletableFuture.supplyAsync(() -> {
						if (obj instanceof DiskSpillingList dsl) {
							if (dsl.isSpilled()) {
								log.debug("ProcessReduce::: Spilled Write Started..." + tasktoprocess);
								Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
								log.debug("ProcessReduce::: Spilled Write Completed" + tasktoprocess);
							} else {
								log.debug("ProcessReduce::: NotSpilled {} {}", dsl.isSpilled(), tasktoprocess);
								diskspilllistinterm.addAll(dsl.getData());
								log.debug("ProcessReduce::: NotSpilled Completed {} {}", dsl.isSpilled(),
										tasktoprocess);
							}
							dsl.clear();
						} else if (obj instanceof byte[] data) {
							diskspilllistinterm
									.addAll((Collection<?>) Utils.convertBytesToObjectCompressed(data, null));
						}
						return null;
					}, fjpool).get();							
					
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			}
			if (object.getTerminiatingclass() == Dummy.class) {
				dummysize++;
				log.debug("ProcessReduce::ShuffleSize {} , Terminating Dummy Size {} {}", terminatingsize, dummysize, tasktoprocess);
			} else if (object.getTerminiatingclass() == DiskSpillingList.class
					|| object.getTerminiatingclass() == ShuffleBlock.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialshufflesize++;
				log.debug("ProcessReduce::InitialShuffleSize {} , Terminating Size {}", initialshufflesize, terminatingsize);
			}
			if (dummysize == terminatingsize || initialshufflesize == terminatingsize) {
				if (diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();
				}
				CompletableFuture.supplyAsync(() -> {
					try {
						if (CollectionUtils.isEmpty(childpipes)) {
							Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
									? (Stream<Tuple2>) Utils.getStreamData(diskspilllistinterm)
									: diskspilllistinterm.getData().stream();
							try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);
									var fsdos = new ByteArrayOutputStream();
									var sos = new SnappyOutputStream(fsdos);
									var output = new Output(sos);) {
								Utils.getKryo().writeClassAndObject(output, streammap.collect(Collectors.toList()));
								output.flush();
								tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
								cacheAble(fsdos);
							} catch (Exception ex) {
								log.error(DataSamudayaConstants.EMPTY, ex);
							}
							jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess),
									true);
						} else {
							log.debug("Reduce Started {}", tasktoprocess);
							diskspilllist = new DiskSpillingList(tasktoprocess, diskspillpercentage,
									DataSamudayaConstants.EMPTY, false, false, false, null, null, 0);
							try {
								Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
										? (Stream<Tuple2>) Utils.getStreamData(diskspilllistinterm)
										: diskspilllistinterm.getData().stream();
								try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(),
										datastream);) {
									streammap.forEach(diskspilllist::add);

								}
							} catch (Exception e) {
								log.error(DataSamudayaConstants.EMPTY, e);
							}

							if (diskspilllist.isSpilled()) {
								diskspilllist.close();
							}
							childpipes.stream().forEach(action -> action
									.tell(new OutputObject(diskspilllist, false, false, DiskSpillingList.class)));
							jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess),
									true);
							log.debug("Reduce Completed {}", tasktoprocess);
						}
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					return null;
				}, fjpool).get();
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
