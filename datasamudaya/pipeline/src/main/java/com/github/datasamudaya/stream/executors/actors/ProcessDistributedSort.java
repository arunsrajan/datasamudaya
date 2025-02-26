package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.BTree;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ProcessDistributedSort extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	Cluster cluster = Cluster.get(getContext().getSystem());
	int terminatingsize;
	int initialsize = 0;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	Task tasktoprocess;
	Cache cache;
	JobStage js;
	private boolean iscacheable = true;
	int btreesize;
	int diskspillpercentage;
	DiskSpillingList diskspilllistinterm;
	List ldiskspill;
	public ProcessDistributedSort(JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<ActorSelection> childpipes, int terminatingsize) {
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
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, diskspillpercentage, null, true, false, false, null,
				null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processDistributedSort).build();
	}

	private void processDistributedSort(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
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
				log.info("processDistributedSort::Started InitialSize {} , Terminating Size {}", initialsize,
						terminatingsize);
				if(diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();		
				}
				List<Task> predecessors = tasktoprocess.getTaskspredecessor();
				List<FieldCollationDirection> fcsc = (List<FieldCollationDirection>) tasktoprocess.getFcsc();				
				Stream<?> datastream = null;				
				NodeIndexKey root = null;
				BTree btree = new BTree(btreesize);
				String key = Utils.getIntermediateResultFS(tasktoprocess);
				for (Object diskspill1 : ldiskspill) {
					Set<NodeIndexKey> diskspillsetintm2 = new TreeSet<>();
					if (diskspill1 instanceof DiskSpillingList<?> dsl) {
						datastream = Utils.getStreamData(dsl);
					} else if (diskspill1 instanceof DiskSpillingSet<?> dss) {
						datastream = Utils.getStreamData(dss);
					} else if (diskspill1 instanceof TreeSet<?> ts) {
						diskspillsetintm2 = (Set<NodeIndexKey>) ts;
						datastream = diskspillsetintm2.stream();
					}
					AtomicInteger atomindex = new AtomicInteger(0);
					if (nonNull(datastream)) {
						datastream.forEach(obj -> {
							if (obj instanceof NodeIndexKey nik) {
								btree.insert(nik, fcsc);
							} else {
								NodeIndexKey nik = new NodeIndexKey(tasktoprocess.getHostport(),
										atomindex.getAndIncrement(), Utils.getKeyFromNodeIndexKey(fcsc, (Object[]) obj),
										obj, null, null, key, tasktoprocess);
								btree.insert(nik, fcsc);
							}
						});
					}
				}				
				if (CollectionUtils.isNotEmpty(childpipes)) {
					DiskSpillingList rootniks = new DiskSpillingList<>(tasktoprocess, diskspillpercentage, null, false,
							false, false, null, null, 0);
					btree.traverse(rootniks);
					try {
						if (rootniks.isSpilled()) {
							rootniks.close();
						}
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(rootniks, false, false, NodeIndexKey.class),
								ActorRef.noSender());
					});
				} else {
					List<NodeIndexKey> cachesort = new ArrayList<>();
					btree.traverse(cachesort);
					cache.put(
							tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
									+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(),
							Utils.convertObjectToBytesCompressed(cachesort, null));
				}
				jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN
						+ tasktoprocess.getStageid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), true);
			}
		}
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
