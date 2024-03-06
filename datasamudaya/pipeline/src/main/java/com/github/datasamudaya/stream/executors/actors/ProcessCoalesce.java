package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

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
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

import static java.util.Objects.nonNull;
import static java.util.Objects.isNull;

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
	DiskSpillingList diskspilllist;
	DiskSpillingList<Tuple2> diskspilllistinterm;
	private ProcessCoalesce(Coalesce coalesce, List<ActorSelection> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, org.ehcache.Cache cache, Task task) {
		this.coalesce = coalesce;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		diskspilllistinterm = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, true, false, false);
		diskspilllist = new DiskSpillingList(task, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, false, false, false);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processCoalesce).build();
	}

	private ProcessCoalesce processCoalesce(OutputObject object) throws Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.value())) {
			initialsize++;
			if(object.value() instanceof DiskSpillingList dsl) {
				if(dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
				} else {
					diskspilllistinterm.addAll(dsl.getData());
				}
			} else {
				diskspilllistinterm.add((Tuple2) object.value());
			}
			log.info("InitSize {} TermSize {}", initialsize, terminatingsize);
			if (initialsize == terminatingsize) {
				log.info("InitSize {} TermSize {}", initialsize, terminatingsize);
				Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()?(Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.
				getLocalFilePathForTask(diskspilllistinterm.getTask(), true, false, false))):diskspilllistinterm.getData().stream();
				datastream.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
						(input1, input2) -> coalesce.getCoalescefunction().apply(input1, input2)))
				.entrySet().parallelStream()
				.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
				.forEach(diskspilllist::add);
				final boolean left = isNull(task.joinpos) ? false
						: nonNull(task.joinpos) && task.joinpos.equals("left") ? true : false;
				final boolean right = isNull(task.joinpos) ? false
						: nonNull(task.joinpos) && task.joinpos.equals("right") ? true : false;
				try(DiskSpillingList diskspill = diskspilllist){
					if (Objects.nonNull(pipelines)) {
						pipelines.parallelStream().forEach(downstreampipe -> {
							downstreampipe.tell(new OutputObject(diskspilllist, left, right), ActorRef.noSender());
						});
					} else {
						Stream<Tuple2> datastreamsplilled = diskspilllist.isSpilled()?(Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.
								getLocalFilePathForTask(diskspilllist.getTask(), true, diskspilllist.getLeft(), diskspilllist.getRight()))):diskspilllist.getData().stream();
						try (var fsdos = new ByteArrayOutputStream();
								var sos = new SnappyOutputStream(fsdos);
								var output = new Output(sos);) {
							Utils.getKryo().writeClassAndObject(output, datastreamsplilled.toList());
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
