package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ForkJoinPool;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.apache.logging.log4j.LogManager;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TerminatingActorValue;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.ActorSelection;
import akka.actor.typed.Behavior;
import akka.actor.typed.RecipientRef;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.Cluster;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * Akka Actors for the shuffle operators
 * @author Administrator
 *
 */
public class ProcessShuffle extends AbstractBehavior<Command> implements Serializable {
	private static final long serialVersionUID = -3022437719020754534L;
	org.apache.logging.log4j.Logger log = LogManager.getLogger(ProcessShuffle.class);
	Cluster cluster = Cluster.get(getContext().getSystem());

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<RecipientRef> childpipes;
	int terminatingsize;
	int initialsize;
	int initialshufflesize;
	boolean shufflecompleted;
	Map<Integer, String> fileblockpath = new ConcurrentHashMap<>();
	Map<Integer, Output> outputstream = new ConcurrentHashMap<>();
	Kryo kryo = Utils.getKryo();
	Semaphore lock = new Semaphore(1);
	ForkJoinPool fjpool = null;
	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessShuffle-" + entityId);
	}

	public static Behavior<Command> create(String entityId, Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess,
			List<RecipientRef> childpipes, ForkJoinPool fjpool) {
		return Behaviors.setup(context -> new ProcessShuffle(context,
				jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, fjpool));
	}

	private ProcessShuffle(ActorContext<Command> context, Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess,
			List<RecipientRef> childpipes,ForkJoinPool fjpool) {
		super(context);
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.terminatingsize = tasktoprocess.parentterminatingsize;
		this.fjpool = fjpool;
	}

	public static record BlocksLocationRecord(FileSystem hdfs, List<ActorSelection> pipeline) implements Serializable {
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processShuffle)
				.onMessage(EntityRefStop.class, this::behaviorStop)
				.build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}
	
	private Behavior<Command> processShuffle(OutputObject object) throws Exception {
		CompletableFuture.supplyAsync(() -> {
			try {
				if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
					if (object.getValue() instanceof TerminatingActorValue tav) {
						this.terminatingsize = tav.getTerminatingval();
					} else if (object.getValue() instanceof ShuffleBlock sb) {
						FilePartitionId fpid = (FilePartitionId) Utils.convertBytesToObject(sb.getPartitionId());
						lock.acquire();
						if (isNull(fileblockpath.get(fpid.getPartitionNumber()))) {
							fileblockpath.put(fpid.getPartitionNumber(), Utils.getLocalFilePathForTask(tasktoprocess,
									DataSamudayaConstants.EMPTY + fpid.getPartitionNumber(), false, false, false));
							var fos = new FileOutputStream(fileblockpath.get(fpid.getPartitionNumber()), true);
							var sos = new SnappyOutputStream(fos);
							var output = new Output(sos);
							outputstream.put(fpid.getPartitionNumber(), output);
						}
						lock.release();
						try {
							Object obj = sb.getData();
							Output output = outputstream.get(fpid.getPartitionNumber());
							sb.setData(null);
							sb.setPartitionId(null);
							if (obj instanceof DiskSpillingList dsl) {
								if (dsl.isSpilled()) {
									log.debug("processShuffle::: Spilled Write Started...");
									Utils.copySpilledDataSourceToFileShuffle(dsl, output);
									log.debug("processShuffle::: Spilled Write Completed");
								} else {
									log.debug("processShuffle::: NotSpilled {}", dsl.getData().size());
									Utils.getKryo().writeClassAndObject(output, dsl.getData());
									output.flush();
									log.debug("processShuffle::: NotSpilled Completed size {}", dsl.getData().size());
								}
								dsl.clear();
							} else {
								log.debug("processShuffle::: DataToWrite Started ...");
								Utils.getKryo().writeClassAndObject(output, Utils.convertBytesToObject((byte[]) obj));
								output.flush();
								log.debug("processShuffle::: DataToWrite Ended ...");
							}
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					} else if (object.getValue() instanceof Dummy) {
						initialshufflesize++;
						log.debug("processShuffle::InitialShuffleSize {} , Terminating Size {}", initialshufflesize,
								terminatingsize);
					}
					if (initialshufflesize == terminatingsize && !shufflecompleted) {
						log.debug("processShuffle::InitialSize {} , Terminating Size {}", initialshufflesize,
								terminatingsize);
						log.debug("Shuffle Started");
						if (CollectionUtils.isNotEmpty(childpipes)) {
							outputstream.entrySet().stream().forEach(entry -> entry.getValue().close());
							final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
									: nonNull(tasktoprocess.joinpos) && "left".equals(tasktoprocess.joinpos) ? true
											: false;
							final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
									: nonNull(tasktoprocess.joinpos) && "right".equals(tasktoprocess.joinpos) ? true
											: false;
							childpipes.parallelStream().forEach(childactorsel -> childactorsel
									.tell(new OutputObject(fileblockpath, leftvalue, rightvalue, Map.class)));
							jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess),
									true);
							log.debug("Shuffle Completed");
							shufflecompleted = true;
						}
					}
				}
			} catch (Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
			}
			return null;
		}, fjpool).get();
		return this;

	}
}
