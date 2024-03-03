package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

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
import com.github.datasamudaya.common.functions.LeftOuterJoinPredicate;
import com.github.datasamudaya.common.utils.Utils;
import com.pivovarit.collectors.ParallelCollectors;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

public class ProcessLeftOuterJoin extends AbstractActor {
	protected JobStage jobstage;
	private static Logger log = LoggerFactory.getLogger(ProcessLeftOuterJoin.class);
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	LeftOuterJoinPredicate lojp;
	int terminatingsize;
	int initialsize = 0;
	List<ActorSelection> pipelines;
	org.ehcache.Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	OutputObject left;
	OutputObject right;

	private ProcessLeftOuterJoin(LeftOuterJoinPredicate lojp, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, org.ehcache.Cache cache, Task task) {
		this.lojp = lojp;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processLeftOuterJoin).build();
	}

	private ProcessLeftOuterJoin processLeftOuterJoin(OutputObject oo) throws Exception {
		if (oo.left()) {
			left = nonNull(oo.value())?oo:left;
		} else if(oo.right()) {
			right = nonNull(oo.value())?oo:right;
		}
		if (nonNull(left) && nonNull(right) && isNull(jobidstageidtaskidcompletedmap.get(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
		+ DataSamudayaConstants.HYPHEN + task.getTaskid()))) {
			final boolean leftvalue = isNull(task.joinpos)?false:nonNull(task.joinpos)&&task.joinpos.equals("left")?true:false;
			final boolean rightvalue = isNull(task.joinpos)?false:nonNull(task.joinpos)&&task.joinpos.equals("right")?true:false;
			try (var seq1 = Seq.of(((List) left.value()).toArray());
					var seq2 = Seq.of(((List) right.value()).toArray());
					var join = seq1.leftOuterJoin(seq2, lojp)) {
				List joinpairsout = join.toList();
				Object[] origobjarray = (Object[]) ((List) right.value()).get(0);
				Object[][] nullobjarr = new Object[2][((Object[]) origobjarray[0]).length];
				for (int numvalues = 0; numvalues < nullobjarr[0].length; numvalues++) {
					nullobjarr[1][numvalues] = true;
				}
				List joinresult = (List) joinpairsout.stream().filter(val -> val instanceof Tuple2).map(value -> {
					Tuple2 maprec = (Tuple2) value;
					Object[] rec1 = (Object[]) maprec.v1;
					Object[] rec2 = (Object[]) maprec.v2;
					if (rec2 == null) {
						return new Tuple2(rec1, nullobjarr);
					}
					return maprec;
				}).collect(Collectors.toList());
				
				if (Objects.nonNull(pipelines)) {
					pipelines.parallelStream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(joinresult, leftvalue, rightvalue), ActorRef.noSender());
						downstreampipe.tell(new OutputObject(null, leftvalue, rightvalue), ActorRef.noSender());
					});
				} else {
					try (var fsdos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(fsdos);
							var output = new Output(sos);) {
						Utils.getKryo().writeClassAndObject(output, joinresult);
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
