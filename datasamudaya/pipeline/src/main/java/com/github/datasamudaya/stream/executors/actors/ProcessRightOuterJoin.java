package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.RightOuterJoinPredicate;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

public class ProcessRightOuterJoin extends AbstractActor {
	protected JobStage jobstage;
	private static Logger log = LoggerFactory.getLogger(ProcessRightOuterJoin.class);
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	RightOuterJoinPredicate rojp;
	int terminatingsize;
	int initialsize = 0;
	List<ActorSelection> pipelines;
	org.ehcache.Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	OutputObject left;
	OutputObject right;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllistintermleft;
	DiskSpillingList diskspilllistintermright;

	private ProcessRightOuterJoin(RightOuterJoinPredicate rojp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, org.ehcache.Cache cache, Task task) {
		this.rojp = rojp;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		diskspilllist = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, false, false, false);
		diskspilllistinterm = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, true, false, false);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processRightOuterJoin).build();
	}

	private ProcessRightOuterJoin processRightOuterJoin(OutputObject oo) throws Exception {
		if (oo.left()) {
			if (nonNull(oo.value()) && oo.value() instanceof DiskSpillingList dsl) {
				diskspilllistintermleft = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, true, true, false);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.getData());
				}
			}
		} else if (oo.right()) {
			if (nonNull(oo.value()) && oo.value() instanceof DiskSpillingList dsl) {
				diskspilllistintermright = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, true, false, true);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermright);
				} else {
					diskspilllistintermright.addAll(dsl.getData());
				}
			}
		}
		if (nonNull(diskspilllistintermleft) && nonNull(diskspilllistintermright)
				&& isNull(jobidstageidtaskidcompletedmap.get(task.getJobid() + DataSamudayaConstants.HYPHEN
						+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()))) {
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && task.joinpos.equals("left") ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && task.joinpos.equals("right") ? true : false;
			Stream datastreamleft = diskspilllistintermleft.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), true,
									diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
					: diskspilllistintermleft.getData().stream();
			Stream datastreamright = diskspilllistintermright.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), true,
									diskspilllistintermright.getLeft(), diskspilllistintermright.getRight())))
					: diskspilllistintermright.getData().stream();
			try (var seq1 = Seq.seq(datastreamleft);
					var seq2 = Seq.seq(datastreamright);
					var join = seq1.rightOuterJoin(seq2, rojp)) {
				join.forEach(diskspilllistinterm::add);
				Stream datastreamleftfirstelem = diskspilllistintermleft.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), true,
										diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
						: diskspilllistintermleft.getData().stream();
				Object[] origvalarr = (Object[]) datastreamleftfirstelem.findFirst().get();
				Object[][] nullobjarr = new Object[2][((Object[]) origvalarr[0]).length];
				for (int numvalues = 0; numvalues < nullobjarr[0].length; numvalues++) {
					nullobjarr[1][numvalues] = true;
				}
				diskspilllistinterm.close();
				Stream diskspilllistintermstream = diskspilllistinterm.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), true,
										diskspilllistinterm.getLeft(), diskspilllistinterm.getRight()))): diskspilllistinterm.getData().stream();
				diskspilllistintermstream.filter(val -> val instanceof Tuple2).map(value -> {
					Tuple2 maprec = (Tuple2) value;
					Object[] rec1 = (Object[]) maprec.v1;
					Object[] rec2 = (Object[]) maprec.v2;
					if (rec1 == null) {
						return new Tuple2(nullobjarr, rec2);
					}
					return maprec;
				}).forEach(diskspilllist::add);
				try (DiskSpillingList diskspill = diskspilllist) {
					if (Objects.nonNull(pipelines)) {
						pipelines.parallelStream().forEach(downstreampipe -> {
							downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue),
									ActorRef.noSender());
							downstreampipe.tell(new OutputObject(null, leftvalue, rightvalue), ActorRef.noSender());
						});
					} else {
						Stream<Tuple2> datastream = diskspilllist.isSpilled()
								? (Stream<Tuple2>) Utils.getStreamData(
										new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(), true,
												diskspilllist.getLeft(), diskspilllist.getRight())))
								: diskspilllist.getData().stream();
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
