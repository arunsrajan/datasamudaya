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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * Akka actors for the inner join operators
 * @author Arun
 *
 */
public class ProcessInnerJoin extends AbstractActor implements Serializable {
	Logger log = LoggerFactory.getLogger(ProcessInnerJoin.class);
	
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	JoinPredicate jp;
	int terminatingsize;
	int initialsize;
	List<ActorSelection> pipelines;
	Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistintermleft;
	DiskSpillingList diskspilllistintermright;
	int diskspillpercentage;
	private ProcessInnerJoin(JoinPredicate jp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task) {
		this.jp = jp;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllist = new DiskSpillingList(task, diskspillpercentage, null, false, false, false, null, null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processInnerJoin)
				.build();
	}

	private ProcessInnerJoin processInnerJoin(OutputObject oo) throws Exception {		
		if (oo.isLeft()) {			
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				log.info("In process Inner Join Left {} {}", oo.isLeft(), getIntermediateDataFSFilePath(task));
				diskspilllistintermleft = new DiskSpillingList(task, diskspillpercentage, null, true, true, false, null, null, 0);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.getData());
				}
			}
		} else if (oo.isRight()) {			
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				log.info("In process Inner Join Right {} {}", oo.isRight(), getIntermediateDataFSFilePath(task));
				diskspilllistintermright = new DiskSpillingList(task, diskspillpercentage, null, true, false, true, null, null, 0);
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
			if(diskspilllistintermleft.isSpilled()) {
				diskspilllistintermleft.close();
			}
			if(diskspilllistintermright.isSpilled()) {
				diskspilllistintermright.close();
			}
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;
			Stream<Tuple2> datastreamleft = diskspilllistintermleft.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
					new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null, true,
							diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
					: diskspilllistintermleft.getData().stream();
			Stream<Tuple2> datastreamright = diskspilllistintermright.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
					new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), null, true,
							diskspilllistintermright.getLeft(), diskspilllistintermright.getRight())))
					: diskspilllistintermright.getData().stream();
			try (var seq1 = Seq.seq(datastreamleft);
					var seq2 = Seq.seq(datastreamright);
					var join = seq1.innerJoin(seq2, jp);) {
				join.forEach(diskspilllist::add);
				if(diskspilllist.isSpilled()) {
					diskspilllist.close();
				}
				if (Objects.nonNull(pipelines)) {
					pipelines.forEach(downstreampipe -> {
						try {
							downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, Dummy.class),
									ActorRef.noSender());
						} catch(Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					});
				} else {
					Stream<Tuple2> datastream = diskspilllist.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.getLocalFilePathForTask(
							diskspilllist.getTask(), null, true, diskspilllist.getLeft(), diskspilllist.getRight())))
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
