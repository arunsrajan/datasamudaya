package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

public class ProcessIntersection extends AbstractBehavior<Command> {
	Logger log = LoggerFactory.getLogger(ProcessIntersection.class);
	int terminatingsize;
	int initialsize;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<EntityRef> childpipes;
	Task tasktoprocess;
	Cache cache;
	JobStage js;
	private final boolean iscacheable = true;
	int btreesize;
	int diskspillpercentage;
	List ldiskspill;

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessIntersection-" + entityId);
	}

	public static Behavior<Command> create(String entityId, JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<EntityRef> childpipes, int terminatingsize) {
		return Behaviors.setup(context -> new ProcessIntersection(context, js, cache,
				jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, terminatingsize));
	}

	public ProcessIntersection(ActorContext<Command> context, JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<EntityRef> childpipes, int terminatingsize) {
		super(context);
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		this.cache = cache;
		this.js = js;
		this.btreesize = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.BTREEELEMENTSNUMBER, DataSamudayaConstants.BTREEELEMENTSNUMBER_DEFAULT));
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		ldiskspill = new ArrayList<>();
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder().onMessage(OutputObject.class, this::processIntersection).build();
	}

	private Behavior<Command> processIntersection(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			log.debug("processIntersection::: InitialSize {} terminating size {}", initialsize, terminatingsize);
			if (object.getValue() instanceof DiskSpillingList<?> dsl) {
				ldiskspill.add(dsl);
			} else if (object.getValue() instanceof DiskSpillingSet<?> dss) {
				ldiskspill.add(dss);
			} else if (object.getValue() instanceof TreeSet<?> ts) {
				ldiskspill.add(ts);
			}
			if (object.getTerminiatingclass() == DiskSpillingList.class
					|| object.getTerminiatingclass() == Dummy.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.debug(
						"processIntersection::Started InitialSize {} , Terminating Size {} Predecessors {} childPipes {}",
						initialsize, terminatingsize, tasktoprocess.getTaskspredecessor(), childpipes);
				if (CollectionUtils.isNotEmpty(childpipes)) {
					int index = 0;
					Set<NodeIndexKey> diskspillsetresult = null;
					Set<NodeIndexKey> diskspillsetintm1 = new TreeSet<>();
					Object diskspill = ldiskspill.remove(0);
					Stream<?> datastream = null;
					if (diskspill instanceof DiskSpillingList<?> dsl) {
						datastream = Utils.getStreamData(dsl);
					} else if (diskspill instanceof DiskSpillingSet<?> dss) {
						datastream = Utils.getStreamData(dss);
					} else if (diskspill instanceof TreeSet<?> ts) {
						diskspillsetintm1 = (Set<NodeIndexKey>) ts;
						datastream = null;
					}
					AtomicInteger atomindex = new AtomicInteger(0);
					Set<NodeIndexKey> diskspillsetinterm = diskspillsetintm1;
					if (nonNull(datastream)) {
						datastream.forEach(obj -> {
							if (obj instanceof NodeIndexKey nik) {
								diskspillsetinterm.add(nik);
							} else {
								diskspillsetinterm.add(new NodeIndexKey(tasktoprocess.getHostport(),
										atomindex.getAndIncrement(), null, obj, null, null, null, tasktoprocess));
							}
						});
					}
					for (Object diskspill1 : ldiskspill) {
						index++;
						diskspillsetresult = new TreeSet<>();
						Set<NodeIndexKey> diskspillsetintm2 = new TreeSet<>();
						Set<NodeIndexKey> diskspillsetinterm2 = diskspillsetintm2;
						if (diskspill1 instanceof DiskSpillingList<?> dsl) {
							datastream = Utils.getStreamData(dsl);
						} else if (diskspill1 instanceof DiskSpillingSet<?> dss) {
							datastream = Utils.getStreamData(dss);
						} else if (diskspill1 instanceof TreeSet<?> ts) {
							diskspillsetintm2 = (Set<NodeIndexKey>) ts;
							datastream = null;
						}
						AtomicInteger atomindex1 = new AtomicInteger(0);
						if (nonNull(datastream)) {
							datastream.forEach(obj -> {
								if (obj instanceof NodeIndexKey nik) {
									diskspillsetinterm2.add(nik);
								} else {
									diskspillsetinterm2.add(new NodeIndexKey(tasktoprocess.getHostport(),
											atomindex1.getAndIncrement(), null, obj, null, null, null, tasktoprocess));
								}
							});
						}
						Iterator<NodeIndexKey> it1 = diskspillsetintm1.iterator();
						Iterator<NodeIndexKey> it2 = diskspillsetintm2.iterator();

						if (!it1.hasNext() || !it2.hasNext()) {
							break; // One of the sets is empty

						}
						NodeIndexKey item1 = it1.next();
						NodeIndexKey item2 = it2.next();

						while (it1.hasNext() && it2.hasNext()) {
							int compare = item1.compareTo(item2);
							if (compare == 0) {
								// Add to result, advance both
								diskspillsetresult.add(item1);
								item1 = it1.next();
								item2 = it2.next();
							} else if (compare < 0) {
								// item1 is smaller, advance it1
								item1 = it1.next();
							} else {
								// item2 is smaller, advance it2
								item2 = it2.next();
							}
						}

						// Check last pair if any
						if (item1.compareTo(item2) == 0) {
							diskspillsetresult.add(item1);
						}
						diskspillsetintm1 = diskspillsetresult;
					}
					log.debug("Object To be sent to downstream pipeline size {}", diskspillsetresult.size());
					Set<NodeIndexKey> diskspillsetresultfinal = diskspillsetresult;
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspillsetresultfinal, false, false, DiskSpillingSet.class));
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
				jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN
						+ tasktoprocess.getStageid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), true);
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
