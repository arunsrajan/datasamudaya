package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;

import org.apache.hadoop.fs.FileSystem;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

public class ProcessCoalesce extends AbstractActor {
	protected JobStage jobstage;
	private static Logger log = LoggerFactory.getLogger(ProcessCoalesce.class);
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	java.util.List<Tuple2> result = new java.util.Vector<>();
	java.util.List<Tuple2> resultcollector = new java.util.Vector<>();
	Coalesce coalesce;
	int terminatingsize;
	int initialsize = 0;
	List<ActorSelection> pipelines;
	org.ehcache.Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;

	private ProcessCoalesce(Coalesce coalesce, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, org.ehcache.Cache cache, Task task) {
		this.coalesce = coalesce;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processCoalesce).build();
	}

	private ProcessCoalesce processCoalesce(OutputObject oo) {
		if (Objects.nonNull(oo) && Objects.nonNull(oo.value())) {
			result.add((Tuple2) oo.value());
			result.parallelStream()
					.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
							(input1, input2) -> coalesce.getCoalescefunction().apply(input1, input2)))
					.entrySet().parallelStream()
					.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
					.forEach(resultcollector::add);
			result.clear();
			result.addAll(resultcollector);
			resultcollector.clear();
		} else {
			initialsize++;
			log.info("InitSize {} TermSize {}", initialsize, terminatingsize);
			if (initialsize == terminatingsize) {
				log.info("InitSize {} TermSize {}", initialsize, terminatingsize);
				if (Objects.nonNull(pipelines)) {
					pipelines.parallelStream().forEach(downstreampipe -> {
						result.forEach(value -> {
							downstreampipe.tell(new OutputObject(value), ActorRef.noSender());
						});
						downstreampipe.tell(new OutputObject(null), ActorRef.noSender());
					});
				} else {
					try (var fsdos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(fsdos);
							var output = new Output(sos);) {
						Utils.getKryo().writeClassAndObject(output, result);
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
