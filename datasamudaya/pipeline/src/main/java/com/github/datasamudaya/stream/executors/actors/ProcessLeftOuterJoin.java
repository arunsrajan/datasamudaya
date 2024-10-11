package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
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
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * Akka actors for the left outer join operators
 * @author arun
 *
 */
public class ProcessLeftOuterJoin extends AbstractActor implements Serializable {
	private static Logger log =LoggerFactory.getLogger(ProcessLeftOuterJoin.class);

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	LeftOuterJoinPredicate lojp;
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
	private ProcessLeftOuterJoin(LeftOuterJoinPredicate lojp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task) {
		this.lojp = lojp;
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
				.match(OutputObject.class, this::processLeftOuterJoin)
				.build();
	}

	private ProcessLeftOuterJoin processLeftOuterJoin(OutputObject oo) throws Exception {
		log.debug("ProcessLeftOuterJoin {} {} {}", oo.getValue().getClass(), oo.isLeft(), oo.isRight());
		if (oo.isLeft()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermleft = new DiskSpillingList(task, diskspillpercentage, null, true, true, false, null, null, 0);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.getData());
				}
			}
		} else if (oo.isRight()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
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
			log.debug("Left Outer Join Task {} from hostport {}", task, task.hostport);
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
					var join = seq1.leftOuterJoin(seq2, lojp)) {
				join.forEach(diskspilllistinterm::add);
				if(diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();
				}
				Stream datastreamrightfirstelem = diskspilllistintermright.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
						new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), null, true,
								diskspilllistintermright.getLeft(), diskspilllistintermright.getRight()))) : diskspilllistintermright.getData().stream();
				Optional optionalfirstelement = datastreamrightfirstelem.findFirst();
				Object[][] nullobjarr;
				if(optionalfirstelement.isPresent()) {
					Object[] origobjarray = (Object[]) optionalfirstelement.get();			
					nullobjarr = new Object[2][((Object[]) origobjarray[0]).length];
					for (int numvalues = 0;numvalues < nullobjarr[0].length;numvalues++) {
						nullobjarr[1][numvalues] = true;
					}					
				} else {
					nullobjarr = new Object[2][1];
					for (int numvalues = 0;numvalues < nullobjarr[0].length;numvalues++) {
						nullobjarr[1][numvalues] = true;
					}	
				}				
				Stream diskspilllistintermstream = diskspilllistinterm.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null,
										true, diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
						: diskspilllistinterm.getData().stream();
				diskspilllistintermstream.filter(val -> val instanceof Tuple2).map(value -> {
					Tuple2 maprec = (Tuple2) value;
					Object[] rec1 = (Object[]) maprec.v1;
					Object[] rec2 = (Object[]) maprec.v2;
					if (rec2 == null) {
						return new Tuple2(rec1, nullobjarr);
					}
					return maprec;
				}).forEach(diskspilllist::add);
				if (diskspilllist.isSpilled()) {
					diskspilllist.close();
				}
				try {
					log.debug("processLeftOuterJoin::: DownStream Pipelines {}", pipelines);
					if (CollectionUtils.isNotEmpty(pipelines)) {
						pipelines.stream().forEach(downstreampipe -> {
							downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, DiskSpillingList.class),
									ActorRef.noSender());
						});
					} else {
						Stream<Tuple2> datastream = diskspilllist.isSpilled()
								? (Stream<Tuple2>) Utils.getStreamData(
								new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(), null, true,
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
				} catch(Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
				jobidstageidtaskidcompletedmap.put(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
						+ DataSamudayaConstants.HYPHEN + task.getTaskid(), true);
				return this;
			} catch(Exception ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
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
