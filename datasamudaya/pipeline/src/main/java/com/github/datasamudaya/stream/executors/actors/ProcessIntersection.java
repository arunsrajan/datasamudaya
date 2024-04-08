package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
					DiskSpillingSet<NodeIndexKey> diskspillsetresult = null;
					
					DiskSpillingSet<NodeIndexKey> diskspillsetintm1 = new DiskSpillingSet(tasktoprocess,
							diskspillpercentage,
							DataSamudayaConstants.INTERMEDIATE + DataSamudayaConstants.HYPHEN + index, false, false,
							false, null, null, 1);
					try (RemoteIteratorClient client = new RemoteIteratorClient(predecessors.remove(0), null,
							RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
						while (client.hasNext()) {
							log.info("Getting Next List From Remote Server");
							List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
									.convertBytesToObjectCompressed((byte[]) client.next(), null);
							log.info("Next List From Remote Server with size {} {}", niks.size(), niks.get(0));
							niks.stream().forEach(diskspillsetintm1::add);
							log.info("DiskSpillSet Intermediate1 {}", niks.size());
						}
					}
					log.info("DiskSpillSet Intermediate1 Closing...");					
					if(diskspillsetintm1.isSpilled()) {
						diskspillsetintm1.close();			
					}
					log.info("DiskSpillSet Intermediate1 Closed with remaining pred {}...", predecessors);
					for (Task predecessor : predecessors) {
						index++;
						diskspillsetresult = new DiskSpillingSet(tasktoprocess, diskspillpercentage,
								index<predecessors.size()-1?(DataSamudayaConstants.INTERMEDIATERESULT + DataSamudayaConstants.HYPHEN + index):null, false, false, false, null, null, 1);
						log.info("Getting Next List From Remote Server with FCD {}", predecessor);
						DiskSpillingSet<NodeIndexKey> diskspillsetintm2 = new DiskSpillingSet(tasktoprocess,
								diskspillpercentage,
								DataSamudayaConstants.INTERMEDIATE + DataSamudayaConstants.HYPHEN + index, false, false,
								false, null, null, 1);
						try (RemoteIteratorClient client = new RemoteIteratorClient(predecessor, null,
								RequestType.LIST, IteratorType.SORTORUNIONORINTERSECTION)) {
							while (client.hasNext()) {
								log.info("Getting Next List From Remote Server");
								List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils
										.convertBytesToObjectCompressed((byte[]) client.next(), null);
								log.info("Next List From Remote Server with size {} {}", niks.size(), niks.get(0));
								niks.stream().forEach(diskspillsetintm2::add);
							}
						}
						if(diskspillsetintm2.isSpilled()) {
							diskspillsetintm2.close();
						}
						log.info("DiskSpillSetIntermediate1 Object {} To be sent to downstream pipeline size {}", diskspillsetintm1.isSpilled(), !diskspillsetintm1.isSpilled()?diskspillsetintm1.size():null);
						log.info("DiskSpillSetIntermediate2 Object {} To be sent to downstream pipeline size {}", diskspillsetintm2.isSpilled(), !diskspillsetintm2.isSpilled()?diskspillsetintm2.size():null);
						DiskSpillingSet<NodeIndexKey> diskspillsetintm1final = diskspillsetintm1;
						Stream<NodeIndexKey> datastream1 = diskspillsetintm1.isSpilled()
								? (Stream) Utils.getStreamData(new FileInputStream(Utils.getLocalFilePathForTask(
										diskspillsetintm1.getTask(), diskspillsetintm1.getAppendwithpath(), diskspillsetintm1.getAppendintermediate(), diskspillsetintm1.getLeft(), diskspillsetintm1.getRight())))
								: diskspillsetintm1.getData().stream().peek(nik->nik.setTask(diskspillsetintm1final.getTask()));

						Stream<NodeIndexKey> datastream2 = diskspillsetintm2.isSpilled()
								? (Stream) Utils.getStreamData(new FileInputStream(Utils
										.getLocalFilePathForTask(diskspillsetintm2.getTask(), diskspillsetintm2.getAppendwithpath(), diskspillsetintm2.getAppendintermediate(), diskspillsetintm2.getLeft(), diskspillsetintm2.getRight())))
								: (Stream<NodeIndexKey>)(diskspillsetintm2.getData().stream().peek(nik->nik.setTask(diskspillsetintm2.getTask())));
						((Map<NodeIndexKey,List<NodeIndexKey>>)Stream.concat(datastream1, datastream2).collect(Collectors.groupingBy(nik->nik, Collectors.mapping(nik->nik, Collectors.toList()))))
			            .entrySet().stream()
			            .filter(e -> e.getValue().size() > 1)
			            .map(e -> e.getKey())
			            .forEach(diskspillsetresult::add);
						if(diskspillsetresult.isSpilled()) {
							diskspillsetresult.close();
						}
						
						diskspillsetintm1 = diskspillsetresult;
					}
					log.info("DiskSpill Object {} To be sent to downstream pipeline size {}", diskspillsetresult.isSpilled(), !diskspillsetresult.isSpilled()?diskspillsetresult.size():null);
					DiskSpillingSet<NodeIndexKey> diskspillsetresultfinal = diskspillsetresult;
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspillsetresultfinal, false, false, DiskSpillingSet.class),
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
