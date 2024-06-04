package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
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
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static java.util.Objects.nonNull;

public class ProcessUnion extends AbstractActor {
	Logger log = LoggerFactory.getLogger(ProcessUnion.class);	
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
	List ldiskspill;

	public ProcessUnion(JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
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
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processUnion).build();
	}

	private void processUnion(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			log.info("processUnion::: {}",object.getValue().getClass());
			if (object.getValue() instanceof DiskSpillingList dsl) {
				ldiskspill.add(dsl);
			} else if (object.getValue() instanceof DiskSpillingSet dss) {
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
				log.info("processUnion::Started InitialSize {} , Terminating Size {} Predecessors {} childPipes {}", initialsize,
						terminatingsize, tasktoprocess.getTaskspredecessor(), childpipes);
				List<Task> predecessors = tasktoprocess.getTaskspredecessor();
				if (CollectionUtils.isNotEmpty(childpipes)) {										
					DiskSpillingSet<NodeIndexKey> diskspillset = new DiskSpillingSet(tasktoprocess, diskspillpercentage, null, false,false ,false, null, null, 1);
					for(Object diskspill:ldiskspill) {
						Stream<?> datastream = null;
						if(diskspill instanceof DiskSpillingList dsl) {
							datastream = Utils.getStreamData(dsl);
						} else if(diskspill instanceof DiskSpillingSet dss) {
							datastream = Utils.getStreamData(dss);
						} else if (diskspill instanceof TreeSet<?> ts) {
							diskspillset.addAll((Set<NodeIndexKey>) ts);
							ts.clear();
							datastream = null;
						}
						try {
							AtomicInteger index = new AtomicInteger(0);
							if(nonNull(datastream)) {
								datastream.forEach(obj -> {
									if (obj instanceof NodeIndexKey nik) {
										diskspillset.add(nik);
									} else {
										diskspillset.add(new NodeIndexKey(tasktoprocess.getHostport(),
												index.getAndIncrement(), null, obj, null, null, null, tasktoprocess));
									}
	
								});
							}

						} catch(Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
						if(diskspill instanceof DiskSpillingList dsl) {
							dsl.clear();
						} else if(diskspill instanceof DiskSpillingSet dss) {
							dss.clear();
						}
					}		
					if(diskspillset.isSpilled()) {
						diskspillset.close();
					}
					childpipes.stream().forEach(downstreampipe -> {
						log.info("Pushing data to downstream");
						downstreampipe.tell(new OutputObject(diskspillset, false, false, DiskSpillingSet.class),
								ActorRef.noSender());
					});
				} else {
					AtomicInteger index = new AtomicInteger(0);
					List<NodeIndexKey> niks = new ArrayList<>();
					for (Task predecessor : predecessors) {
						NodeIndexKey nik = new NodeIndexKey(predecessor.getHostport(), index.getAndIncrement(),
								null,null, null, null, null, predecessor);
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
