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
import org.ehcache.Cache;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
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

/**
 * Akka Actors for the Right Join Operators
 * @author Administrator
 *
 */
public class ProcessRightOuterJoin extends AbstractActor implements Serializable {
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
	RightOuterJoinPredicate rojp;
	int terminatingsize;
	int initialsize;
	List<ActorSelection> pipelines;
	Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	OutputObject left;
	OutputObject right;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllistintermleft;
	DiskSpillingList diskspilllistintermright;
	int diskspillpercentage;

	private ProcessRightOuterJoin(RightOuterJoinPredicate rojp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task) {
		this.rojp = rojp;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllist = new DiskSpillingList(task, diskspillpercentage, null, false, false, false, null, null, 0);
		diskspilllistinterm = new DiskSpillingList(task, diskspillpercentage, null, true, false, false, null, null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processRightOuterJoin)
				.build();
	}

	private ProcessRightOuterJoin processRightOuterJoin(OutputObject oo) throws Exception {
		if (oo.isLeft()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermleft = new DiskSpillingList(task, diskspillpercentage, null, true, true, false, null, null, 0);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.readListFromBytes());
				}
			}
		} else if (oo.isRight()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermright = new DiskSpillingList(task, diskspillpercentage, null, true, false, true, null, null, 0);
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
					: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;
			Stream datastreamleft = diskspilllistintermleft.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
					new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null, true,
							diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
					: diskspilllistintermleft.readListFromBytes().stream();
			Stream datastreamright = diskspilllistintermright.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
					new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), null, true,
							diskspilllistintermright.getLeft(), diskspilllistintermright.getRight())))
					: diskspilllistintermright.readListFromBytes().stream();
			try (var seq1 = Seq.seq(datastreamleft);
					var seq2 = Seq.seq(datastreamright);
					var join = seq1.rightOuterJoin(seq2, rojp)) {
				join.forEach(diskspilllistinterm::add);
				Stream datastreamleftfirstelem = diskspilllistintermleft.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
						new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null, true,
								diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
						: diskspilllistintermleft.readListFromBytes().stream();
				Object[] origvalarr = (Object[]) datastreamleftfirstelem.findFirst().get();
				Object[][] nullobjarr = new Object[2][((Object[]) origvalarr[0]).length];
				for (int numvalues = 0;numvalues < nullobjarr[0].length;numvalues++) {
					nullobjarr[1][numvalues] = true;
				}
				diskspilllistinterm.close();
				Stream diskspilllistintermstream = diskspilllistinterm.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
						new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true,
								diskspilllistinterm.getLeft(), diskspilllistinterm.getRight()))) : diskspilllistinterm.readListFromBytes().stream();
				diskspilllistintermstream.filter(val -> val instanceof Tuple2).map(value -> {
					Tuple2 maprec = (Tuple2) value;
					Object[] rec1 = (Object[]) maprec.v1;
					Object[] rec2 = (Object[]) maprec.v2;
					if (rec1 == null) {
						return new Tuple2(nullobjarr, rec2);
					}
					return maprec;
				}).forEach(diskspilllist::add);
				diskspilllist.close();
				try (DiskSpillingList diskspill = diskspilllist) {
					if (Objects.nonNull(pipelines)) {
						pipelines.parallelStream().forEach(downstreampipe -> {
							downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, Dummy.class),
									ActorRef.noSender());
							downstreampipe.tell(new OutputObject(new Dummy(), leftvalue, rightvalue, Dummy.class), ActorRef.noSender());
						});
					} else {
						Stream<Tuple2> datastream = diskspilllist.isSpilled()
								? (Stream<Tuple2>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(), null, true,
										diskspilllist.getLeft(), diskspilllist.getRight())))
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
