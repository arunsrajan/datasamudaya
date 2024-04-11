package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.IteratorType;
import com.github.datasamudaya.common.utils.RemoteIteratorClient;
import com.github.datasamudaya.common.utils.RequestType;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

public class ProcessIntersection extends AbstractActor {
	Logger log = LoggerFactory.getLogger(ProcessIntersection.class);
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
	DiskSpillingSet diskspillset;
	public ProcessIntersection(JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
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
		diskspillset = new DiskSpillingSet(tasktoprocess, diskspillpercentage, null, false, false, false, null,
				null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processIntersection).build();
	}

	private void processIntersection(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingSet dss) {
				log.info("Is ProcessIntersection Spilled {} {}", dss.isSpilled(), dss.getTask());
				if (!dss.isSpilled()) {
					Utils.copyDiskSpillingSetToDisk(dss);
				}
				dss.clear();
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.info(
						"processIntersection::Started InitialSize {} , Terminating Size {} Predecessors {} childPipes {}",
						initialsize, terminatingsize, tasktoprocess.getTaskspredecessor(), childpipes);
				List<Task> predecessors = tasktoprocess.getTaskspredecessor();
				if (CollectionUtils.isNotEmpty(childpipes)) {
					int index = 0;
					Set<NodeIndexKey> diskspillsetresult = null;
					
					Set<NodeIndexKey> diskspillsetintm1 = new TreeSet<>();
					try (RemoteIteratorClient client = new RemoteIteratorClient(predecessors.remove(0), null,
							RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
						while (client.hasNext()) {
							List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
									.convertBytesToObjectCompressed((byte[]) client.next(), null);
							diskspillsetintm1.addAll(niks);
						}
					}					
					for (Task predecessor : predecessors) {
						index++;
						diskspillsetresult = new TreeSet<>();
						Set<NodeIndexKey> diskspillsetintm2 = new TreeSet<>();
						try (RemoteIteratorClient client = new RemoteIteratorClient(predecessor, null,
								RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
							while (client.hasNext()) {
								List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
										.convertBytesToObjectCompressed((byte[]) client.next(), null);
								diskspillsetintm2.addAll(niks);
							}
						}
						Iterator<NodeIndexKey> it1 = diskspillsetintm1.iterator();
			            Iterator<NodeIndexKey> it2 = diskspillsetintm2.iterator();

			            if (!it1.hasNext() || !it2.hasNext()) break; // One of the sets is empty

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
					diskspillset.addAll(diskspillsetresult);
					if(diskspillset.isSpilled()) {
						diskspillset.close();
					}
					log.info("Object To be sent to downstream pipeline size {}", diskspillsetresult.size());
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspillset, false, false, DiskSpillingSet.class),
								ActorRef.noSender());
					});
				} else {
					AtomicInteger index = new AtomicInteger(0);
					List<NodeIndexKey> niks = new ArrayList<>();
					for (Task predecessor : predecessors) {
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
