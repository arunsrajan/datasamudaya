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
import java.util.concurrent.Semaphore;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TerminatingActorValue;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;

import akka.actor.AbstractActor;
import akka.actor.Actor;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ProcessShuffle extends AbstractActor implements Serializable {
		LoggingAdapter log = Logging.getLogger(getContext().getSystem(), this);
	  Cluster cluster = Cluster.get(getContext().getSystem());

	  // subscribe to cluster changes
	  @Override
	  public void preStart() {
	    // #subscribe
	    cluster.subscribe(
	        getSelf(), ClusterEvent.initialStateAsEvents(), 
	        MemberEvent.class, UnreachableMember.class,
	        MemberUp.class, MemberUp.class);
	    // #subscribe
	  }

	  // re-subscribe when restart
	  @Override
	  public void postStop() {
	    cluster.unsubscribe(getSelf());
	  }
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	int terminatingsize;
	int initialsize = 0;
	int initialshufflesize = 0;
	boolean shufflecompleted = false;
	Map<Integer, String> fileblockpath=new ConcurrentHashMap<>();
	Map<Integer, Output> outputstream=new ConcurrentHashMap<>();
	Kryo kryo = Utils.getKryo();
	Semaphore lock = new Semaphore(1);
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
		return receiveBuilder()
				.match(OutputObject.class, this::processShuffle)
				.match(
			            MemberUp.class,
			            mUp -> {
			              log.info("Member is Up: {}", mUp.member());
			            })
			        .match(
			            UnreachableMember.class,
			            mUnreachable -> {
			              log.info("Member detected as unreachable: {}", mUnreachable.member());
			            })
			        .match(
			            MemberRemoved.class,
			            mRemoved -> {
			              log.info("Member is Removed: {}", mRemoved.member());
			            })
				.build();
	}

	private void processShuffle(OutputObject object) throws Exception {		
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if(object.getValue() instanceof TerminatingActorValue tav) {
				this.terminatingsize = tav.getTerminatingval();
			}
			else if(object.getValue() instanceof ShuffleBlock sb) {
				lock.acquire();
				FilePartitionId fpid = (FilePartitionId) Utils.convertBytesToObject(sb.getPartitionId());
				if(isNull(fileblockpath.get(fpid.getPartitionNumber()))) {
					fileblockpath.put(fpid.getPartitionNumber(), Utils.getLocalFilePathForTask(tasktoprocess, 
							DataSamudayaConstants.EMPTY+fpid.getPartitionNumber(), false,false,false)
							);
					var fos = new FileOutputStream(fileblockpath.get(fpid.getPartitionNumber()), true);
					var sos = new SnappyOutputStream(fos);
					var output = new Output(sos);
					outputstream.put(fpid.getPartitionNumber(), output);
				}
				try{
					Object obj = sb.getData();
					Output output = outputstream.get(fpid.getPartitionNumber());
					if(obj instanceof DiskSpillingList dsl) {						
						if(dsl.isSpilled()) {
							Utils.copySpilledDataSourceToFileShuffle(dsl, output);
						} else {
							Utils.getKryo().writeClassAndObject(output, dsl.readListFromBytes());
							output.flush();
						}
					} else {
						Utils.getKryo().writeClassAndObject(output, Utils.convertBytesToObject((byte[]) obj));
						output.flush();
					}
				}catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				} finally {
					lock.release();
				}
			} else if(object.getValue() instanceof Dummy) {
				lock.acquire();
				initialshufflesize++;
				log.info("processShuffle::InitialShuffleSize {} , Terminating Size {}", initialshufflesize, terminatingsize);
				lock.release();
			}
			if (initialshufflesize == terminatingsize && !shufflecompleted) {
				log.info("processShuffle::InitialSize {} , Terminating Size {}", initialshufflesize, terminatingsize);
				log.info("Shuffle Started");
				if (CollectionUtils.isNotEmpty(childpipes)) {
					outputstream.entrySet().stream().forEach(entry->entry.getValue().close());
					final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("left") ? true : false;
					final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("right") ? true : false;					
					childpipes.parallelStream().forEach(childactorsel->childactorsel.tell(new OutputObject(fileblockpath, leftvalue, rightvalue), Actor.noSender()));
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
					log.info("Shuffle Completed");
					shufflecompleted = true;
				}
			}
		}

		
	}
}
