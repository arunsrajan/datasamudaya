package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ProcessInnerJoin extends AbstractActor implements Serializable {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	  Cluster cluster = Cluster.get(getContext().getSystem());

	  // subscribe to cluster changes
	  @Override
	  public void preStart() {
	    // #subscribe
		  cluster.subscribe(
			        getSelf(), ClusterEvent.initialStateAsEvents(), 
			        MemberEvent.class, UnreachableMember.class,
			        MemberUp.class, MemberUp.class);
	  }

	  // re-subscribe when restart
	  @Override
	  public void postStop() {
	    cluster.unsubscribe(getSelf());
	  }
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	JoinPredicate jp;
	int terminatingsize;
	int initialsize = 0;
	List<ActorSelection> pipelines;
	org.ehcache.Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistintermleft;
	DiskSpillingList diskspilllistintermright;

	private ProcessInnerJoin(JoinPredicate jp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, org.ehcache.Cache cache, Task task) {
		this.jp = jp;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		diskspilllist = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null,  false, false, false, null, null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processInnerJoin)
				.match(
			            MemberUp.class,
			            mUp -> {
			              log.info("Member is Up: {}", mUp.member());
			            })
			        .match(
			            UnreachableMember.class,
			            mUnreachable -> {
			              log.info("Member detected as unreachable: {}", mUnreachable.member());
			            })
			        .match(
			            MemberRemoved.class,
			            mRemoved -> {
			              log.info("Member is Removed: {}", mRemoved.member());
			            })
				.build();
	}

	private ProcessInnerJoin processInnerJoin(OutputObject oo) throws Exception {
		if (oo.isLeft()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermleft = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, true, true, false, null, null, 0);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.readListFromBytes());
				}
			}
		} else if (oo.isRight()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermright = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, true, false, true, null, null, 0);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermright);
				} else {
					diskspilllistintermright.addAll(dsl.readListFromBytes());
				}
			}
		}
		if (nonNull(diskspilllistintermleft) && nonNull(diskspilllistintermright)
				&& isNull(jobidstageidtaskidcompletedmap.get(task.getJobid() + DataSamudayaConstants.HYPHEN
						+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()))) {
			diskspilllistintermleft.close();
			diskspilllistintermright.close();
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && task.joinpos.equals("left") ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && task.joinpos.equals("right") ? true : false;
			Stream<Tuple2> datastreamleft = diskspilllistintermleft.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null, true,
									diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
					: diskspilllistintermleft.readListFromBytes().stream();
			Stream<Tuple2> datastreamright = diskspilllistintermright.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), null, true,
									diskspilllistintermright.getLeft(), diskspilllistintermright.getRight())))
					: diskspilllistintermright.readListFromBytes().stream();
			try (var seq1 = Seq.seq(datastreamleft);
					var seq2 = Seq.seq(datastreamright);
					var join = seq1.innerJoin(seq2, jp);
					DiskSpillingList diskspill = diskspilllist) {
				join.forEach(diskspilllist::add);
				diskspilllist.close();
				if (Objects.nonNull(pipelines)) {
					pipelines.parallelStream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue),
								ActorRef.noSender());
						downstreampipe.tell(new OutputObject(null, leftvalue, rightvalue), ActorRef.noSender());
					});
				} else {
					Stream<Tuple2> datastream = diskspilllist.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.getLocalFilePathForTask(
									diskspilllist.getTask(), null, true, diskspilllist.getLeft(), diskspilllist.getRight())))
							: diskspilllist.readListFromBytes().stream();
					try (var fsdos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(fsdos);
							var output = new Output(sos);) {
						Utils.getKryo().writeClassAndObject(output, datastream.toList());
						output.flush();
						task.setNumbytesgenerated(fsdos.toByteArray().length);
						byte[] bt = ((ByteArrayOutputStream) fsdos).toByteArray();
						cache.put(getIntermediateDataFSFilePath(task), bt);
					} catch (Exception ex) {
						log.error("Error in putting output in cache", ex);
					}
				}
				jobidstageidtaskidcompletedmap.put(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
						+ DataSamudayaConstants.HYPHEN + task.getTaskid(), true);
				return this;
			}
		}
		return this;
	}

	/**
	 * This method gets the path in jobid-stageid-taskid.
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}
}
