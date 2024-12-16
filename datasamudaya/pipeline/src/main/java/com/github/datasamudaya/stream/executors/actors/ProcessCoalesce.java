package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Vector;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.Coalesce;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.Cluster;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * The Akka Actors for the coalesce operators
 * @author arun
 *
 */
public class ProcessCoalesce extends AbstractBehavior<Command> implements Serializable {
	Logger log = LoggerFactory.getLogger(ProcessCoalesce.class);
	Cluster cluster = Cluster.get(getContext().getSystem());

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	List<Tuple2> result = new Vector<>();
	List<Tuple2> resultcollector = new Vector<>();
	Coalesce coalesce;
	int terminatingsize;
	int initialsize;
	List<EntityRef> pipelines;
	Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	DiskSpillingList diskspilllist;
	DiskSpillingList<Tuple2> diskspilllistinterm;

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessCoalesce-" + entityId);
	}

	public static Behavior<Command> create(String entityId, Coalesce coalesce, List<EntityRef> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task) {
		return Behaviors.setup(context -> new ProcessCoalesce(context, coalesce, pipelines, terminatingsize,
				jobidstageidtaskidcompletedmap,
				cache, task));
	}

	private ProcessCoalesce(ActorContext<Command> context, Coalesce coalesce, List<EntityRef> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task) {
		super(context);
		this.coalesce = coalesce;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		int diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllistinterm = new DiskSpillingList(task, diskspillpercentage, null, true, false, false, null, null, 0);
		diskspilllist = new DiskSpillingList(task, diskspillpercentage, null, false, false, false, null, null, 0);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processCoalesce)
				.build();
	}

	private Behavior<Command> processCoalesce(OutputObject object) throws Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
				} else {
					diskspilllistinterm.addAll(dsl.getData());
				}
				dsl.clear();
			}
			if (object.getTerminiatingclass() == Dummy.class || object.getTerminiatingclass() == DiskSpillingList.class) {
				initialsize++;
			}
			log.debug("processCoalesce::: InitSize {} TermSize {}", initialsize, terminatingsize);
			if (initialsize == terminatingsize) {
				log.debug("processCoalesce::: InitSize {} TermSize {}", initialsize, terminatingsize);
				if (diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();
				}
				Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
						? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(
						Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true, false, false)))
						: diskspilllistinterm.getData().stream();
				datastream
						.collect(Collectors.toMap(Tuple2::v1, Tuple2::v2,
								(input1, input2) ->
										coalesce.getCoalescefunction().apply(input1, input2)))
						.entrySet().stream()
						.map(entry -> Tuple.tuple(((Entry) entry).getKey(), ((Entry) entry).getValue()))
						.forEach(diskspilllist::add);
				if (diskspilllist.isSpilled()) {
					diskspilllist.close();
				}
				final boolean left = isNull(task.joinpos) ? false
						: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
				final boolean right = isNull(task.joinpos) ? false
						: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;
				if (CollectionUtils.isNotEmpty(pipelines)) {
					log.debug("Process Coalesce To Pipeline Started {} IsSpilled {} {}", pipelines, diskspilllist.isSpilled(), diskspilllist.getData());
					pipelines.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspilllist, left, right, DiskSpillingList.class));
					});
					log.debug("Process Coalesce To Pipeline Ended {}", pipelines);
				} else {
					log.debug("Process Coalesce To Cache Started");
					Stream<Tuple2> datastreamsplilled = diskspilllist.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(), null,
									true, diskspilllist.getLeft(), diskspilllist.getRight())))
							: diskspilllist.getData().stream();
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
					log.debug("Process Coalesce To Cache Ended");
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
