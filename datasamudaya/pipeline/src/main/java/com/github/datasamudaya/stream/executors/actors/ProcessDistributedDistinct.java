package com.github.datasamudaya.stream.executors.actors;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
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

public class ProcessDistributedDistinct extends AbstractActor {
	LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	Cluster cluster = Cluster.get(getContext().getSystem());
	int terminatingsize;
	int initialsize = 0;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	Task tasktoprocess;
	int diskspillpercentage;
	DiskSpillingSet diskspillset;

	public ProcessDistributedDistinct(Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<ActorSelection> childpipes, int terminatingsize) {
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.tasktoprocess = tasktoprocess;
		this.terminatingsize = terminatingsize;
		this.childpipes = childpipes;
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE, DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));
		diskspillset = new DiskSpillingSet(tasktoprocess, diskspillpercentage, null, false, false, false, null,
				null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processDistributedDistinct).build();
	}

	private void processDistributedDistinct(OutputObject object) throws PipelineException, Exception {		
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				log.info("In Distributed Distinct {} {} {} {}", object, dsl.size(), dsl.isSpilled(), dsl.getTask());
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspillset);
				} else {
					diskspillset.addAll(dsl.readListFromBytes());
				}
				dsl.clear();				
			} else if(object.getValue() instanceof Dummy) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.info("processDistributedDistinct::Started InitialSize {} , Terminating Size {} childPipes {} Task {}", initialsize,
						terminatingsize, childpipes, diskspillset.getTask());
				diskspillset.close();				
				if (CollectionUtils.isNotEmpty(childpipes)) {															
					log.info("processDistributedDistinct::DiskSpill intermediate Set Size {} Task {}", diskspillset.size(), diskspillset.getTask());
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspillset, false, false, DiskSpillingSet.class),
								ActorRef.noSender());
					});
				}
				jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN
						+ tasktoprocess.getStageid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), true);
			}
		}
	}
}
