package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.OutputStream;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.xerial.snappy.SnappyInputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.FieldCollationDirection;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.FieldCollatedSortedComparator;
import com.github.datasamudaya.common.utils.RemoteListIteratorClient;
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

	public ProcessDistributedSort(JobStage js, Cache cache, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<ActorSelection> childpipes, int terminatingsize) {
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		this.cache = cache;
		this.js = js;
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processDistributedSort).build();
	}

	private void processDistributedSort(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.info("processDistributedSort::Started InitialSize {} , Terminating Size {}", initialsize,
						terminatingsize);

				List<Task> predecessors = tasktoprocess.getTaskspredecessor();
				List<FieldCollationDirection> fcsc = (List<FieldCollationDirection>) tasktoprocess.getFcsc();
				Kryo kryo = Utils.getKryo();
				NodeIndexKey root = null;
				for (Task predecessor : predecessors) {
					if (isNull(predecessor.getHostport())) {
						String key = Utils.getIntermediateResultFS(predecessor);
						try (var bais = new ByteArrayInputStream(
								(byte[]) cache.get(key));
								var sis = new SnappyInputStream(bais);
								var input = new Input(sis);) {
							List<Object[]> out = (List) kryo.readClassAndObject(input);
							int index = 0;
							if (CollectionUtils.isNotEmpty(out)) {
								if (isNull(root)) {
									root = new NodeIndexKey(predecessor.getHostport(), index,
											Utils.getKeyFromNodeIndexKey(fcsc, out.get(0)), out.get(0), null,
											null, key, predecessor);
								}
								out.remove(0);
								for (Object[] obj : out) {
									NodeIndexKey child = new NodeIndexKey(predecessor.getHostport(), index,
											Utils.getKeyFromNodeIndexKey(fcsc, obj), obj, null, null, key, predecessor);
									Utils.formSortedBinaryTree(root, child, fcsc);
									index++;
								}
							}
						}
					} else {
						try (RemoteListIteratorClient client = new RemoteListIteratorClient(predecessor, fcsc)) {
							while (client.hasNext()) {
								NodeIndexKey nik = (NodeIndexKey) client.next();
								nik.setTask(predecessor);
								if (isNull(root)) {
									root = nik;
								} else {
									Utils.formSortedBinaryTree(root, nik, fcsc);
								}
							}
						}
					}
				}
				if (CollectionUtils.isNotEmpty(childpipes)) {
					NodeIndexKey rootnik = root;
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(rootnik, false, false, NodeIndexKey.class), ActorRef.noSender());
					});
				} else {
					cache.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getStageid()
						+ DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), Utils.convertObjectToBytesCompressed(root, null));
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
