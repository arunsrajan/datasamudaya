package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaAkkaNodesTaskExecutor;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.MapFunction;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.NodeIndexKeyComparator;
import com.github.datasamudaya.common.utils.ObjectArrayComparator;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.Address;
import akka.actor.typed.Behavior;
import akka.actor.typed.RecipientRef;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

/**
 * Akka actors for the union operators
 */
public class ProcessUnion extends AbstractBehavior<Command> {
	Logger log = LoggerFactory.getLogger(ProcessUnion.class);
	int terminatingsize;
	int initialsize;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<RecipientRef> childpipes;
	Task tasktoprocess;
	Cache cache;
	JobStage js;
	private final boolean iscacheable = true;
	int btreesize;
	int diskspillpercentage;
	List ldiskspill;
	ExecutorService es;
	EntityTypeKey<Command> entitytypekey;
	
	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessUnion-" + entityId);
	}

	public static Behavior<Command> create(String entityId, JobStage js, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<RecipientRef> childpipes,
			int terminatingsize, ExecutorService es) {
		return Behaviors.setup(context -> new ProcessUnion(context, js, cache, jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, terminatingsize, es));
	}

	public ProcessUnion(ActorContext<Command> context, JobStage js, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<RecipientRef> childpipes,
			int terminatingsize, ExecutorService es) {
		super(context);
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		this.cache = cache;
		this.js = js;
		this.es = es;
		this.entitytypekey =  createTypeKey(tasktoprocess.getJobid()+tasktoprocess.getStageid()+tasktoprocess.getTaskid());
		this.btreesize = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.BTREEELEMENTSNUMBER, DataSamudayaConstants.BTREEELEMENTSNUMBER_DEFAULT));
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		ldiskspill = new ArrayList<>();
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(OutputObject.class, this::processUnion)
				.onMessage(EntityRefStop.class, this::behaviorStop).build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}

	private Behavior<Command> processUnion(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			log.debug("processUnion::: {}", object.getValue().getClass());
			if (object.getValue() instanceof DiskSpillingList dsl) {
				ldiskspill.add(dsl);
			} else if (object.getValue() instanceof DiskSpillingSet dss) {
				ldiskspill.add(dss);
			} else if (object.getValue() instanceof TreeSet<?> ts) {
				ldiskspill.add(ts);
			}
			if (object.getTerminiatingclass() == DiskSpillingList.class || object.getTerminiatingclass() == Dummy.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialsize++;
			}
			CompletableFuture.supplyAsync(() -> {
				try {
					if (initialsize == terminatingsize) {
						log.debug(
								"processUnion::Started InitialSize {} , Terminating Size {} Predecessors {} childPipes {}",
								initialsize, terminatingsize, tasktoprocess.getTaskspredecessor(), childpipes);
						List<Task> predecessors = tasktoprocess.getTaskspredecessor();
						if (CollectionUtils.isNotEmpty(childpipes)) {
							DiskSpillingSet<NodeIndexKey> diskspillset = new DiskSpillingSet(tasktoprocess,
									diskspillpercentage, null, false, false, false, null, null, 1, true,
									new NodeIndexKeyComparator());
							Address address = getContext().getSystem().address();
							String hostportactorsystem = address.getHost().get() + DataSamudayaConstants.COLON + address.getPort().get();
							String tehp = DataSamudayaAkkaNodesTaskExecutor.get().get(hostportactorsystem);
							if(!diskspillset.getTask().getHostport().equals(tehp)) {
								diskspillset.getTask().setHostport(tehp);
							}
							List<Stream<?>> datastreamlist = new ArrayList<>();
							for (Object diskspill : ldiskspill) {
								Stream<?> datastream = null;
								if (diskspill instanceof DiskSpillingList dsl) {
									datastream = Utils.getStreamData(dsl);
								} else if (diskspill instanceof DiskSpillingSet dss) {
									datastream = Utils.getStreamData(dss);
								} else if (diskspill instanceof TreeSet<?> ts) {
									datastream = ts.stream();
								}
								datastreamlist.add(datastream);
							}
							try {
								AtomicInteger index = new AtomicInteger(0);
								if (CollectionUtils.isNotEmpty(datastreamlist)) {
									datastreamlist.stream()
									.flatMap(stream -> stream).map(obj->{
											if (obj instanceof NodeIndexKey nik) {												
												return (Object[])nik.getValue();
											}
											return (Object[])obj;
									}).map(new MapFunction<Object[], List<Object>>(){
										private static final long serialVersionUID = -8710155646864668081L;

										@Override
										public List<Object> apply(Object[] obj) {
											return Arrays.asList(obj);
										}
									}).distinct().map(lst->lst.toArray())
									.sorted(new ObjectArrayComparator())
									.forEach(obj -> {										
										diskspillset.add(new NodeIndexKey(tasktoprocess.getHostport(),
											index.getAndIncrement(), null, obj, null, null, null,
											tasktoprocess));
									});
								}

							} catch (Exception ex) {
								log.error(DataSamudayaConstants.EMPTY, ex);
							}
							if (diskspillset.isSpilled()) {
								diskspillset.close();
							}
							childpipes.stream().forEach(downstreampipe -> {
								log.debug("Pushing data to downstream");
								downstreampipe
										.tell(new OutputObject(diskspillset, false, false, DiskSpillingSet.class));
							});
						} else {
							AtomicInteger index = new AtomicInteger(0);
							List<NodeIndexKey> niks = new ArrayList<>();
							for (Task predecessor : predecessors) {
								NodeIndexKey nik = new NodeIndexKey(predecessor.getHostport(), index.getAndIncrement(),
										null, null, null, null, null, predecessor);
								niks.add(nik);
							}
							cache.put(
									tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
											+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(),
									Utils.convertObjectToBytesCompressed(niks, null));
						}
						jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN
								+ tasktoprocess.getStageid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(),
								true);
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
				return null;
			}, es).get();
		} else {
			if (object.getTerminiatingclass() == DiskSpillingList.class || object.getTerminiatingclass() == Dummy.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				if (CollectionUtils.isNotEmpty(childpipes)) {
					childpipes.stream().forEach(downstreampipe -> {
						log.debug("Pushing data to downstream");
						downstreampipe.tell(new OutputObject(
								new DiskSpillingSet(tasktoprocess, diskspillpercentage, null, false, false, false, null,
										null, 1, true, new NodeIndexKeyComparator()),
								false, false, DiskSpillingSet.class));
					});
				} else {
					AtomicInteger index = new AtomicInteger(0);
					List<NodeIndexKey> niks = new ArrayList<>();
					for (Task predecessor : tasktoprocess.getTaskspredecessor()) {
						NodeIndexKey nik = new NodeIndexKey(predecessor.getHostport(), index.getAndIncrement(), null,
								null, null, null, null, predecessor);
						niks.add(nik);
					}
					cache.put(
							tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
									+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(),
							Utils.convertObjectToBytesCompressed(niks, null));
				}
			}
		}
		return this;
	}

	/**
	 * cachable stream
	 * 
	 * @param fsdos
	 */
	@SuppressWarnings("unchecked")
	public void cacheAble(OutputStream fsdos) {
		if (iscacheable) {
			byte[] bt = ((ByteArrayOutputStream) fsdos).toByteArray();
			cache.put(getIntermediateDataFSFilePath(tasktoprocess), bt);
		}
	}

	/**
	 * This method gets the path in jobid-stageid-taskid.
	 */
	public String getIntermediateDataFSFilePath(Task task) {
		return task.jobid + DataSamudayaConstants.HYPHEN + task.stageid + DataSamudayaConstants.HYPHEN + task.taskid;
	}
}
