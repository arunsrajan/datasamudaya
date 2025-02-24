package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Optional;
import java.util.Vector;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.hadoop.fs.FileSystem;
import org.ehcache.Cache;
import org.jooq.lambda.Seq;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaAkkaNodesTaskExecutor;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.Join;
import com.github.datasamudaya.common.functions.JoinPredicate;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingMap;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.utils.SQLUtils;

import akka.actor.Address;
import akka.actor.typed.Behavior;
import akka.actor.typed.RecipientRef;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * Akka actors for the inner join operators
 * @author Arun
 *
 */
public class ProcessInnerJoin extends AbstractBehavior<Command> implements Serializable {
	private static final long serialVersionUID = -7346719449401289317L;

	Logger log = LoggerFactory.getLogger(ProcessInnerJoin.class);

	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	Join join;
	int terminatingsize;
	int initialsize;
	List<RecipientRef> pipelines;
	Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	DiskSpillingList diskspilllist;
	DiskSpillingMap<List<Object>,List<Object[]>> diskspilllistintermleft;
	DiskSpillingMap<List<Object>,List<Object[]>> diskspilllistintermright;
	int diskspillpercentage;
	ExecutorService es;
	EntityTypeKey<Command> entitytypekey;
	
	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessInnerJoin-" + entityId);
	}

	public static Behavior<Command> create(String entityId, Join join, List<RecipientRef> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task, ExecutorService es) {
		return Behaviors.setup(context -> new ProcessInnerJoin(context, join, pipelines, terminatingsize,
				jobidstageidtaskidcompletedmap, cache, task, es));
	}


	private ProcessInnerJoin(ActorContext<Command> context, Join join, List<RecipientRef> pipelines, int terminatingsize,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task, ExecutorService es) {
		super(context);
		this.join = join;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		this.es = es;
		this.entitytypekey =  createTypeKey(task.getJobid()+task.getStageid()+task.getTaskid());
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllist = new DiskSpillingList(task, diskspillpercentage, null, false, false, false, null, null, 0);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processInnerJoin)
				.onMessage(EntityRefStop.class, this::behaviorStop)
				.build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}
	
	private Behavior<Command> processInnerJoin(OutputObject oo) throws Exception {
		if (oo.isLeft()) {
			if (nonNull(oo.getValue()) && (oo.getValue() instanceof DiskSpillingMap dsm)) {
				this.diskspilllistintermleft = dsm;
			}
		} else if (oo.isRight()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingMap dsm) {
				this.diskspilllistintermright = dsm;
			}
		}
		if (nonNull(diskspilllistintermleft) && nonNull(diskspilllistintermright)
				&& isNull(jobidstageidtaskidcompletedmap.get(task.getJobid() + DataSamudayaConstants.HYPHEN
				+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()))) {
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;			
			try {
				Address address = getContext().getSystem().address();
				String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON + address.getPort().get();
				String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
				if(!diskspilllist.getTask().getHostport().equals(tehp)) {
					diskspilllist.getTask().setHostport(tehp);
				}	
				CompletableFuture.supplyAsync(() -> {
					try {						
						Stream<Tuple2<Object[], Object[]>> streamresult = null; 
						if (diskspilllistintermleft.size() <= diskspilllistintermright.size()) {
							Stream<Entry<List<Object>, List<Object[]>>> datastreamleft = diskspilllistintermleft.entrySet().stream();
							streamresult = datastreamleft.flatMap(entryleft -> {
								List<Object[]> rightvalues = diskspilllistintermright.get(entryleft.getKey());
								if (CollectionUtils.isNotEmpty(rightvalues)) {
									Seq seq1 = Seq.seq(entryleft.getValue().stream());
									Seq seq2 = Seq.seq(rightvalues.stream());
									Seq innerjoin = seq1.innerJoin(seq2, join.getJp());
									return innerjoin.stream();
								} 
								return Arrays.asList().stream();
							});
						} else {
							Stream<Entry<List<Object>, List<Object[]>>> datastreamleft = diskspilllistintermright.entrySet().stream();
							streamresult = datastreamleft.flatMap(entryright -> {
									List<Object[]> leftvalues = diskspilllistintermleft.get(entryright.getKey());
									if (CollectionUtils.isNotEmpty(leftvalues)) {
										Seq seq1 = Seq.seq(leftvalues.stream());
										Seq seq2 = Seq.seq(entryright.getValue().stream());
										Seq innerjoin = seq1.innerJoin(seq2, join.getJp());
										return innerjoin.stream();
									}
									return Arrays.asList().stream();
								
							});
						}
						final DiskSpillingMap mapdownstream;
						if(CollectionUtils.isNotEmpty(join.getJoinkeys())) {
							mapdownstream = streamresult.map(new MapFunction<Tuple2<Object[], Object[]>, Object[]>() {
								private static final long serialVersionUID = 7259968444353464945L;

								@Override
								public Object[] apply(Tuple2<Object[], Object[]> tup2) {

									return new Object[]{SQLUtils.concatenate(((Object[]) tup2.v1()[0]), ((Object[]) tup2.v2()[0])),
											SQLUtils.concatenate(((Object[]) tup2.v1()[1]), ((Object[]) tup2.v2()[1]))};
								}
							}).collect(
									Collectors.groupingBy(
									(Object[] objarr) -> 
									Arrays.asList(SQLUtils.extractMapKeysFromJoinKeys((Object[])objarr[0], join.getJoinkeys())), ()->new DiskSpillingMap<>(task, "interm"),
									Collectors.mapping(objarr -> objarr,Collectors.toCollection(Vector::new))));
						} else {
							mapdownstream = null;
							streamresult.forEach(
							result -> diskspilllist.add(result));
							if (diskspilllist.isSpilled()) {
								diskspilllist.close();
							}
						}						
						if (Objects.nonNull(pipelines)) {
							pipelines.forEach(downstreampipe -> {
								try {
									if(nonNull(mapdownstream)) {
										downstreampipe
										.tell(new OutputObject(mapdownstream, leftvalue, rightvalue, DiskSpillingMap.class));
									} else {
										downstreampipe
											.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, Dummy.class));
									}
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
							});
						} else {
							Stream<Tuple2> datastream = diskspilllist.isSpilled()
									? (Stream<Tuple2>) Utils.getStreamData(
											new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(),
													null, true, diskspilllist.getLeft(), diskspilllist.getRight())))
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
				catch(Exception ex) {
					log.error("Error in putting output in cache", ex);
				}
				return null;
				}, es).get();
				jobidstageidtaskidcompletedmap.put(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
						+ DataSamudayaConstants.HYPHEN + task.getTaskid(), true);
				Utils.updateZookeeperTasksData(task, true);
				return this;
			}catch(Exception ex) {
				log.error("Error in putting output in cache", ex);
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
