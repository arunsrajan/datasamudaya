package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.BTree;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.RemoteListIteratorClient;
import com.github.datasamudaya.common.utils.RequestType;
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
	public ProcessDistributedSort(JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<ActorSelection> childpipes, int terminatingsize) {
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		this.cache = cache;
		this.js = js;
		this.btreesize = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.BTREEELEMENTSNUMBER, 
				DataSamudayaConstants.BTREEELEMENTSNUMBER_DEFAULT));
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, diskspillpercentage, null, true, false, false, null, null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processDistributedSort).build();
	}

	private void processDistributedSort(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				if (dsl.isSpilled()) {
					log.info("In Distributed Sort Spilled {}", dsl.isSpilled());
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
				} else {					
					diskspilllistinterm.addAll(dsl.readListFromBytes());
				}
				dsl.clear();
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.info("processDistributedSort::Started InitialSize {} , Terminating Size {}", initialsize,
						terminatingsize);
				diskspilllistinterm.close();
				List<Task> predecessors = tasktoprocess.getTaskspredecessor();
				List<FieldCollationDirection> fcsc = (List<FieldCollationDirection>) tasktoprocess.getFcsc();
				Kryo kryo = Utils.getKryo();
				NodeIndexKey root = null;
				BTree btree = new BTree(btreesize);
				
					if (isNull(tasktoprocess.getHostport()) || predecessors.size() == 1 || !diskspilllistinterm.isSpilled()) {
						String key = Utils.getIntermediateResultFS(tasktoprocess);
						try (Stream<Object[]> datastream = diskspilllistinterm.isSpilled()
								? (Stream<Object[]>) Utils.getStreamData(new FileInputStream(Utils
										.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true, false, false)))
								: diskspilllistinterm.readListFromBytes().stream()) {
							AtomicInteger index = new AtomicInteger(0);
														
							datastream.forEach(obj-> {
									NodeIndexKey nik = new NodeIndexKey(tasktoprocess.getHostport(), index.getAndIncrement(),
											Utils.getKeyFromNodeIndexKey(fcsc, obj), obj, null, null, key, tasktoprocess);
									btree.insert(nik, fcsc);
								});
							
						}
					} else {
						for (Task predecessor : predecessors) {
						try (RemoteListIteratorClient client = new RemoteListIteratorClient(predecessor, fcsc, RequestType.LIST)) {
							while (client.hasNext()) {
								log.info("Getting Next List From Remote Server");
								List<NodeIndexKey> niks = (List<NodeIndexKey>) Utils.convertBytesToObjectCompressed((byte[]) client.next(), null);
								log.info("Next List From Remote Server with size {}", niks.size());
								for(NodeIndexKey nik:niks) {
									nik.setTask(predecessor);
									btree.insert(nik, fcsc);
								}
							}
						}
					}
				}
				DiskSpillingList rootniks = new DiskSpillingList<>(tasktoprocess, diskspillpercentage, null, false, false, false, null, null, 0);
				btree.traverse(rootniks);
				try {
					rootniks.close();
				} catch (Exception e) {
					log.error(DataSamudayaConstants.EMPTY, e);
				}
				if (CollectionUtils.isNotEmpty(childpipes)) {
					childpipes.stream().forEach(downstreampipe -> {						
						downstreampipe.tell(new OutputObject(rootniks, false, false, NodeIndexKey.class), ActorRef.noSender());
					});
				} else {
					Stream<NodeIndexKey> datastream = rootniks.isSpilled()
							? (Stream<NodeIndexKey>) Utils.getStreamData(new FileInputStream(Utils
									.getLocalFilePathForTask(rootniks.getTask(), null, false, false, false)))
							: rootniks.readListFromBytes().stream();
					cache.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
						+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), Utils.convertObjectToBytesCompressed(datastream.toList(), null));
				}
				jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
				+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), true);
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
