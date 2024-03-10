package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.FileOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorSelection;

public class ProcessShuffle extends AbstractActor {
	protected JobStage jobstage;
	private static org.slf4j.Logger log = LoggerFactory.getLogger(ProcessShuffle.class);
	protected FileSystem hdfs;
	protected boolean completed;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	int terminatingsize;
	int initialsize = 0;
	Map<Integer, String> fileblockpath=new ConcurrentHashMap<>();
	Kryo kryo = Utils.getKryo();
	private ProcessShuffle(Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<ActorSelection> childpipes) {
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.terminatingsize = tasktoprocess.parentterminatingsize;
	}

	public static record BlocksLocationRecord(FileSystem hdfs, List<ActorSelection> pipeline) implements Serializable {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processShuffle).build();
	}

	private void processShuffle(OutputObject object) throws Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.value())) {			
			if(object.value() instanceof ShuffleBlock sb) {
				if(isNull(fileblockpath.get(sb.partitionId().getPartitionNumber()))) {
					fileblockpath.put(sb.partitionId().getPartitionNumber(), Utils.getLocalFilePathForTask(tasktoprocess, 
							DataSamudayaConstants.EMPTY+sb.partitionId().getPartitionNumber(), false,false,false)
							);
				}
				try(var fos = new FileOutputStream(fileblockpath.get(sb.partitionId().getPartitionNumber()), true);
						var sos = new SnappyOutputStream(fos);
						var output = new Output(sos)){
					DiskSpillingList dsl = (DiskSpillingList) sb.data();
					if(dsl.isSpilled()) {
						Utils.copySpilledDataSourceToFileShuffle(dsl, output);
					} else {
						kryo.writeClassAndObject(output, dsl.getData());
						output.flush();
					}
				}catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
				initialsize++;
			}
			if (initialsize == terminatingsize) {
				if (CollectionUtils.isNotEmpty(childpipes)) {
					final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("left") ? true : false;
					final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("right") ? true : false;					
					childpipes.parallelStream().forEach(childactorsel->childactorsel.tell(new OutputObject(fileblockpath, leftvalue, rightvalue), Actor.noSender()));
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
				}
			}
		}

		
	}
}
