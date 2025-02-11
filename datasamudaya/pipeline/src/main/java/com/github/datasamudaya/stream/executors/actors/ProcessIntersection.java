package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
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
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.utils.NodeIndexKeyComparator;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.typed.Behavior;
import akka.actor.typed.RecipientRef;
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
	List<RecipientRef> childpipes;
	Task tasktoprocess;
	Cache cache;
	JobStage js;
	private final boolean iscacheable = true;
	int btreesize;
	int diskspillpercentage;
	List ldiskspill;
	ExecutorService es;
	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessIntersection-" + entityId);
	}

	public static Behavior<Command> create(String entityId, JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<RecipientRef> childpipes, int terminatingsize, ExecutorService es) {
		return Behaviors.setup(context -> new ProcessIntersection(context, js, cache,
				jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, terminatingsize, es));
	}

	public ProcessIntersection(ActorContext<Command> context, JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<RecipientRef> childpipes, int terminatingsize, ExecutorService es) {
		super(context);
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		this.cache = cache;
		this.js = js;
		this.es = es;
		this.btreesize = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.BTREEELEMENTSNUMBER, DataSamudayaConstants.BTREEELEMENTSNUMBER_DEFAULT));
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		ldiskspill = new ArrayList<>();
	}

	@Override
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processIntersection)
				.onMessage(EntityRefStop.class, this::behaviorStop)
				.build();
	}
	
	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
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
					DiskSpillingSet<NodeIndexKey> diskspillsetresult = null;
					DiskSpillingSet<NodeIndexKey> diskspillsetintm1 = null;
					int totalelements = ldiskspill.size();
					Object diskspill = ldiskspill.remove(0);
					Stream<NodeIndexKey> datastream1 = null;
					Stream<NodeIndexKey> datastream2 = null;
					if (diskspill instanceof DiskSpillingList<?> dsl) {
						datastream1 = (Stream<NodeIndexKey>) Utils.getStreamData(dsl);
					} else if (diskspill instanceof DiskSpillingSet<?> dss) {
						datastream1 = (Stream<NodeIndexKey>) Utils.getStreamData(dss);
					} else if (diskspill instanceof TreeSet<?> ts) {
						datastream1 = (Stream<NodeIndexKey>) ts.stream();
					}
					AtomicInteger atomindexleft = new AtomicInteger(0);
					AtomicInteger atomindexright = new AtomicInteger(0);	
					if(CollectionUtils.isNotEmpty(ldiskspill)) {
						for (Object diskspill1 : ldiskspill) {
							index++;						
							if (diskspill1 instanceof DiskSpillingList<?> dsl) {
								datastream2 = (Stream<NodeIndexKey>) Utils.getStreamData(dsl);
							} else if (diskspill1 instanceof DiskSpillingSet<?> dss) {
								datastream2 = (Stream<NodeIndexKey>) Utils.getStreamData(dss);
							} else if (diskspill1 instanceof TreeSet<?> ts) {
								datastream2 = (Stream<NodeIndexKey>) ts.stream();
							}
							
							Iterator<Object> it1 = null;
							Iterator<Object> it2 = null;
							
							if(nonNull(diskspillsetintm1) && diskspillsetintm1.isSpilled()) {
								diskspillsetintm1.close();
								datastream1 = (Stream<NodeIndexKey>) Utils.getStreamData(diskspillsetintm1);								
							} else if(nonNull(diskspillsetintm1) && !diskspillsetintm1.isSpilled()) {
								datastream1 = diskspillsetintm1.stream();
							}
							if(index < totalelements - 1) {
								diskspillsetresult = new DiskSpillingSet(tasktoprocess,
									diskspillpercentage, index+DataSamudayaConstants.EMPTY, true, false, false, null, null, 1, true, new NodeIndexKeyComparator());
							} else {
								diskspillsetresult = new DiskSpillingSet(tasktoprocess,
										diskspillpercentage, null, false, false, false, null, null, 1, true, new NodeIndexKeyComparator());
							}
							it1 = (Iterator) datastream1.iterator();
							it2 = (Iterator) datastream2.iterator();
							
	
							if (!it1.hasNext() || !it2.hasNext()) {
								break; // One of the sets is empty
	
							}
							
							Object obj1 = it1.hasNext()?it1.next():null;
							Object obj2 = it2.hasNext()?it2.next():null;
							
							NodeIndexKey item1 = null;
							NodeIndexKey item2 = null;
							
							while (it1.hasNext() && it2.hasNext() && nonNull(obj1) && nonNull(obj2)) {
								if(obj1 instanceof NodeIndexKey nik) {
									item1 = nik;
								} else {
									item1 = new NodeIndexKey(tasktoprocess.getHostport(),
											atomindexleft.getAndIncrement(), null, obj1, null, null, null, tasktoprocess);
								}
								if(obj2 instanceof NodeIndexKey nik) {
									item2 = nik;
								} else {
									item2 = new NodeIndexKey(tasktoprocess.getHostport(),
											atomindexright.getAndIncrement(), null, obj2, null, null, null, tasktoprocess);
								}
								int compare = item1.compareTo(item2);
								if (compare == 0) {
									// Add to result, advance both
									diskspillsetresult.add(item1);
									obj1 = it1.hasNext()?it1.next():null;
									obj2 = it2.hasNext()?it2.next():null;
								} else if (compare < 0) {
									// item1 is smaller, advance it1
									obj1 = it1.hasNext()?it1.next():null;
								} else {
									// item2 is smaller, advance it2
									obj2 = it2.hasNext()?it2.next():null;
								}
							}
							if(obj1 instanceof NodeIndexKey nik) {
								item1 = nik;
							} else {
								item1 = new NodeIndexKey(tasktoprocess.getHostport(),
										atomindexleft.getAndIncrement(), null, obj1, null, null, null, tasktoprocess);
							}
							if(obj2 instanceof NodeIndexKey nik) {
								item2 = nik;
							} else {
								item2 = new NodeIndexKey(tasktoprocess.getHostport(),
										atomindexright.getAndIncrement(), null, obj2, null, null, null, tasktoprocess);
							}
							// Check last pair if any
							if (nonNull(item1) && nonNull(item2) && item1.compareTo(item2) == 0) {
								diskspillsetresult.add(item1);
							}
							diskspillsetintm1 = diskspillsetresult;
						}						
					}
					log.debug("Object To be sent to downstream pipeline size {}", diskspillsetresult.size());
					DiskSpillingSet<NodeIndexKey> diskspillsetresultfinal = diskspillsetresult;
					if(diskspillsetresultfinal.isSpilled()) {
						diskspillsetresultfinal.close();
					}
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
