package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.Serializable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Vector;
import java.util.Map.Entry;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
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
import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaAkkaNodesTaskExecutor;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.functions.RightJoin;
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
import akka.cluster.Cluster;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * Akka Actors for the Right Join Operators
 * 
 * @author Administrator
 *
 */
public class ProcessRightOuterJoin extends AbstractBehavior<Command> implements Serializable {
	private static final long serialVersionUID = -3413388366098283314L;
	Logger log = LoggerFactory.getLogger(ProcessRightOuterJoin.class);
	Cluster cluster = Cluster.get(getContext().getSystem());

	// subscribe to cluster changes
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task task;
	boolean iscacheable;
	ExecutorService executor;
	RightJoin rightjoin;
	int terminatingsize;
	int initialsize;
	List<RecipientRef> pipelines;
	Cache cache;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	OutputObject left;
	OutputObject right;
	DiskSpillingList diskspilllist;
	DiskSpillingMap<List<Object>, List<Object[]>> diskspillmapintermleft;
	DiskSpillingMap<List<Object>, List<Object[]>> diskspillmapintermright;
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllistintermleft;
	DiskSpillingList diskspilllistintermright;
	int diskspillpercentage;
	ExecutorService es;
	EntityTypeKey<Command> entitytypekey;

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessRightOuterJoin-" + entityId);
	}

	public static Behavior<Command> create(String entityId, RightJoin rightjoin, List<RecipientRef> pipelines,
			int terminatingsize, Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task,
			ExecutorService es) {
		return Behaviors.setup(context -> new ProcessRightOuterJoin(context, rightjoin, pipelines, terminatingsize,
				jobidstageidtaskidcompletedmap, cache, task, es));
	}

	private ProcessRightOuterJoin(ActorContext<Command> context, RightJoin rightjoin, List<RecipientRef> pipelines,
			int terminatingsize, Map<String, Boolean> jobidstageidtaskidcompletedmap, Cache cache, Task task,
			ExecutorService es) {
		super(context);
		this.rightjoin = rightjoin;
		this.pipelines = pipelines;
		this.terminatingsize = terminatingsize;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.cache = cache;
		this.task = task;
		this.es = es;
		this.entitytypekey = createTypeKey(task.getJobid() + task.getStageid() + task.getTaskid());
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllist = new DiskSpillingList(task, diskspillpercentage, null, false, false, false, null, null, 0);
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(OutputObject.class, this::processRightOuterJoin)
				.onMessage(EntityRefStop.class, this::behaviorStop).build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}

	private Behavior<Command> processRightOuterJoin(OutputObject oo) throws Exception {
		log.debug("ProcessRIghtOuterJoin {} {} {}", oo.getValue().getClass(), oo.isLeft(), oo.isRight());
		if (oo.isLeft()) {
			if (nonNull(oo.getValue()) && (oo.getValue() instanceof DiskSpillingMap dsm)) {
				this.diskspillmapintermleft = dsm;
			} else if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermleft = new DiskSpillingList(task, diskspillpercentage, null, true, true, false, null,
						null, 0);
				Address address = getContext().getSystem().address();
				String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON
						+ address.getPort().get();
				String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
				if (!diskspilllistintermleft.getTask().getHostport().equals(tehp)) {
					diskspilllistintermleft.getTask().setHostport(tehp);
				}
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistintermleft);
				} else {
					diskspilllistintermleft.addAll(dsl.getData());
				}
			}
		} else if (oo.isRight()) {
			if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingMap dsm) {
				this.diskspillmapintermright = dsm;
			} else if (nonNull(oo.getValue()) && oo.getValue() instanceof DiskSpillingList dsl) {
				diskspilllistintermright = new DiskSpillingList(task, diskspillpercentage, null, true, false, true,
						null, null, 0);
				Address address = getContext().getSystem().address();
				String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON
						+ address.getPort().get();
				String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
				if (!diskspilllistintermright.getTask().getHostport().equals(tehp)) {
					diskspilllistintermright.getTask().setHostport(tehp);
				}
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
			if (diskspilllistintermleft.isSpilled()) {
				diskspilllistintermleft.close();
			}
			if (diskspilllistintermright.isSpilled()) {
				diskspilllistintermright.close();
			}
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;
			Stream datastreamleft = diskspilllistintermleft.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null,
									true, diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
					: diskspilllistintermleft.getData().stream();
			Stream datastreamright = diskspilllistintermright.isSpilled()
					? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistintermright.getTask(), null,
									true, diskspilllistintermright.getLeft(), diskspilllistintermright.getRight())))
					: diskspilllistintermright.getData().stream();
			Address address = getContext().getSystem().address();
			String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON
					+ address.getPort().get();
			String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
			if (!diskspilllistinterm.getTask().getHostport().equals(tehp)) {
				diskspilllistinterm.getTask().setHostport(tehp);
			}
			CompletableFuture.supplyAsync(() -> {
				try (var seq1 = Seq.seq(datastreamleft);
						var seq2 = Seq.seq(datastreamright);
						var joinseq = seq1.rightOuterJoin(seq2, rightjoin.getRojp())) {
					joinseq.forEach(diskspilllistinterm::add);
					Stream datastreamleftfirstelem = diskspilllistintermleft.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(new FileInputStream(
									Utils.getLocalFilePathForTask(diskspilllistintermleft.getTask(), null, true,
											diskspilllistintermleft.getLeft(), diskspilllistintermleft.getRight())))
							: diskspilllistintermleft.getData().stream();
					Object[] origvalarr = (Object[]) datastreamleftfirstelem.findFirst().get();
					Object[][] nullobjarr = new Object[2][((Object[]) origvalarr[0]).length];
					for (int numvalues = 0; numvalues < nullobjarr[0].length; numvalues++) {
						nullobjarr[1][numvalues] = true;
					}
					if (diskspilllistinterm.isSpilled()) {
						diskspilllistinterm.close();
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
						if (rec1 == null) {
							return new Tuple2(nullobjarr, rec2);
						}
						return maprec;
					}).forEach(diskspilllist::add);
					if (!diskspilllist.getTask().getHostport().equals(tehp)) {
						diskspilllist.getTask().setHostport(tehp);
					}
					if (diskspilllist.isSpilled()) {
						diskspilllist.close();
					}
					try {
						if (Objects.nonNull(pipelines)) {
							pipelines.parallelStream().forEach(downstreampipe -> {
								downstreampipe.tell(new OutputObject(diskspilllist, leftvalue, rightvalue, Dummy.class));
								downstreampipe.tell(new OutputObject(new Dummy(), leftvalue, rightvalue, Dummy.class));
							});
						} else {
							Stream<Tuple2> datastream = diskspilllist.isSpilled()
									? (Stream<Tuple2>) Utils.getStreamData(
											new FileInputStream(Utils.getLocalFilePathForTask(diskspilllist.getTask(), null,
													true, diskspilllist.getLeft(), diskspilllist.getRight())))
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
					} catch (Exception ex) {
	
					}
					jobidstageidtaskidcompletedmap.put(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
							+ DataSamudayaConstants.HYPHEN + task.getTaskid(), true);
					Utils.updateZookeeperTasksData(task, true);
					return this;
				} catch (Exception ex) {
					log.error("Error in putting output in cache", ex);
				}
				return null;
			}, es).get();
		} else if (nonNull(diskspillmapintermleft) && nonNull(diskspillmapintermright)
				&& isNull(jobidstageidtaskidcompletedmap.get(task.getJobid() + DataSamudayaConstants.HYPHEN
						+ task.getStageid() + DataSamudayaConstants.HYPHEN + task.getTaskid()))) {
			log.debug("Left Outer Join Task {} from hostport {}", task, task.hostport);
			final boolean leftvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "left".equals(task.joinpos) ? true : false;
			final boolean rightvalue = isNull(task.joinpos) ? false
					: nonNull(task.joinpos) && "right".equals(task.joinpos) ? true : false;
			try {
				Address address = getContext().getSystem().address();
				String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON
						+ address.getPort().get();
				String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
				if (!diskspilllist.getTask().getHostport().equals(tehp)) {
					diskspilllist.getTask().setHostport(tehp);
				}
				CompletableFuture.supplyAsync(() -> {
					try {
						Stream<Tuple2<Object[], Object[]>> streamresult = null;
						Stream datastreamleftfirstelem = diskspillmapintermleft.isSpilled()
								? (Stream) Utils.getStreamData(diskspillmapintermleft)
								: diskspillmapintermleft.entrySet().stream();
						Optional optionalfirstelement = datastreamleftfirstelem.findFirst();
						Object[][] nullobjarr;
						if (optionalfirstelement.isPresent()) {
							Entry<List<Object>, List<Object[]>> origobjarray = (Entry<List<Object>, List<Object[]>>) optionalfirstelement
									.get();
							nullobjarr = new Object[2][((Object[]) origobjarray.getValue().get(0)[0]).length];
							for (int numvalues = 0; numvalues < nullobjarr[0].length; numvalues++) {
								nullobjarr[1][numvalues] = true;
							}
						} else {
							nullobjarr = new Object[2][1];
							for (int numvalues = 0; numvalues < nullobjarr[0].length; numvalues++) {
								nullobjarr[1][numvalues] = true;
							}
						}
						datastreamleftfirstelem.close();
						streamresult = diskspillmapintermright.entrySet().stream().flatMap(entryright -> {
							List<Object[]> objectsleft = diskspillmapintermleft.getOrDefault(entryright.getKey(),
									Collections.emptyList());
							if (CollectionUtils.isNotEmpty(objectsleft)) {
								List<Object[]> rightvalueobjarr = (List<Object[]>) entryright.getValue();
								Seq seq1 = Seq.seq(objectsleft.stream());
								Seq seq2 = Seq.seq(rightvalueobjarr.stream());
								Seq rightjoinseq = seq1.rightOuterJoin(seq2, rightjoin.getRojp());
								return rightjoinseq.stream();
							}
							Seq seq1 = Seq.seq(objectsleft.stream());
							Seq seq2 = Seq.seq(entryright.getValue().stream());
							Seq rightjoinseq = seq1.rightOuterJoin(seq2, rightjoin.getRojp());
							return rightjoinseq.stream();
						});
						final DiskSpillingMap mapdownstream;
						if (CollectionUtils.isNotEmpty(rightjoin.getJoinkeys())) {
							mapdownstream = streamresult.map(new MapFunction<Tuple2<Object[], Object[]>, Object[]>() {
								private static final long serialVersionUID = 7259968444353464945L;

								@Override
								public Object[] apply(Tuple2<Object[], Object[]> tup2) {
									Object[] rec1 = (Object[]) tup2.v1;
									if (rec1 == null) {
										return new Object[] {
												SQLUtils.concatenate(nullobjarr[0], ((Object[]) tup2.v2()[0])),
												SQLUtils.concatenate(((Object[]) tup2.v1()[1]), nullobjarr[1]) };
									}
									return new Object[] {
											SQLUtils.concatenate(((Object[]) tup2.v1()[0]), ((Object[]) tup2.v2()[0])),
											SQLUtils.concatenate(((Object[]) tup2.v1()[1]),
													((Object[]) tup2.v2()[1])) };
								}
							}).collect(Collectors.groupingBy(
									(Object[] objarr) -> Arrays.asList(SQLUtils
											.extractMapKeysFromJoinKeys((Object[]) objarr[0], rightjoin.getJoinkeys())),
									() -> new DiskSpillingMap<>(task, "interm"),
									Collectors.mapping(objarr -> objarr, Collectors.toCollection(Vector::new))));
						} else {
							mapdownstream = null;
							streamresult.filter(val -> val instanceof Tuple2).map(value -> {
								Tuple2 maprec = (Tuple2) value;
								Object[] rec1 = (Object[]) maprec.v1;
								Object[] rec2 = (Object[]) maprec.v2;
								if (rec1 == null) {
									return new Tuple2(nullobjarr, rec2);
								}
								return maprec;
							}).forEach(result -> diskspilllist.add(result));
							if (diskspilllist.isSpilled()) {
								diskspilllist.close();
							}
						}
						if (Objects.nonNull(pipelines)) {
							pipelines.forEach(downstreampipe -> {
								try {
									if (nonNull(mapdownstream)) {
										downstreampipe.tell(new OutputObject(mapdownstream, leftvalue, rightvalue,
												DiskSpillingMap.class));
									} else {
										downstreampipe.tell(
												new OutputObject(diskspilllist, leftvalue, rightvalue, Dummy.class));
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
					} catch (Exception ex) {
						log.error("Error in putting output in cache", ex);
					}
					return null;
				}, es).get();
				jobidstageidtaskidcompletedmap.put(task.getJobid() + DataSamudayaConstants.HYPHEN + task.getStageid()
						+ DataSamudayaConstants.HYPHEN + task.getTaskid(), true);
				Utils.updateZookeeperTasksData(task, true);
			} catch (Exception ex) {
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
