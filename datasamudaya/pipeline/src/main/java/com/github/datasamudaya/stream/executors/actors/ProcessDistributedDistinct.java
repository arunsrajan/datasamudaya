package com.github.datasamudaya.stream.executors.actors;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.Command;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.EntityRefStop;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.actor.typed.Behavior;
import akka.actor.typed.javadsl.AbstractBehavior;
import akka.actor.typed.javadsl.ActorContext;
import akka.actor.typed.javadsl.Behaviors;
import akka.actor.typed.javadsl.Receive;
import akka.cluster.Cluster;
import akka.cluster.sharding.typed.javadsl.EntityRef;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;

public class ProcessDistributedDistinct extends AbstractBehavior<Command> {
	Logger log = LoggerFactory.getLogger(ProcessDistributedDistinct.class);
	Cluster cluster = Cluster.get(getContext().getSystem());
	int terminatingsize;
	int initialsize;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<EntityRef> childpipes;
	Task tasktoprocess;
	int diskspillpercentage;
	DiskSpillingSet diskspillset;

	public static EntityTypeKey<Command> createTypeKey(String entityId) {
		return EntityTypeKey.create(Command.class, "ProcessDistributedDistinct-" + entityId);
	}

	public static Behavior<Command> create(String entityId, Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess,
			List<EntityRef> childpipes, int terminatingsize) {
		return Behaviors.setup(context -> new ProcessDistributedDistinct(context,
				jobidstageidtaskidcompletedmap,
				tasktoprocess, childpipes, terminatingsize));
	}


	public ProcessDistributedDistinct(ActorContext<Command> context, Map<String, Boolean> jobidstageidtaskidcompletedmap,
			Task tasktoprocess, List<EntityRef> childpipes, int terminatingsize) {
		super(context);
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
	public Receive<Command> createReceive() {
		return newReceiveBuilder()
				.onMessage(OutputObject.class, this::processDistributedDistinct)
				.onMessage(EntityRefStop.class, this::behaviorStop)
				.build();
	}

	private Behavior<Command> behaviorStop(EntityRefStop stop) {
		return Behaviors.stopped();
	}
	
	private Behavior<Command> processDistributedDistinct(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof DiskSpillingList dsl) {
				log.debug("In Distributed Distinct {} {} {} {} {}", object, dsl.size(), dsl.isSpilled(), dsl.getTask(), terminatingsize);
				if (dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspillset);
				} else {
					diskspillset.addAll(dsl.getData());
				}
				dsl.clear();
			}
			if (object.getTerminiatingclass() == DiskSpillingList.class || object.getTerminiatingclass() == Dummy.class) {
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				log.debug("processDistributedDistinct::Started InitialSize {} , Terminating Size {} childPipes {} Task {}", initialsize,
						terminatingsize, childpipes, diskspillset.getTask());
				if (diskspillset.isSpilled()) {
					diskspillset.close();
				}
				if (CollectionUtils.isNotEmpty(childpipes)) {
					log.debug("processDistributedDistinct::DiskSpill intermediate Set Is Spilled {} Task {}", diskspillset.isSpilled(), diskspillset.getTask());
					childpipes.stream().forEach(downstreampipe -> {
						downstreampipe.tell(new OutputObject(diskspillset, false, false, DiskSpillingSet.class));
					});
				}
				jobidstageidtaskidcompletedmap.put(tasktoprocess.getJobid() + DataSamudayaConstants.HYPHEN
						+ tasktoprocess.getStageid() + DataSamudayaConstants.HYPHEN + tasktoprocess.getTaskid(), true);
			}
		}
		return this;
	}
}
