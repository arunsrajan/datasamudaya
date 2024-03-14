package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.StreamUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.Cluster;
import akka.cluster.ClusterEvent;
import akka.cluster.ClusterEvent.MemberRemoved;
import akka.cluster.ClusterEvent.MemberUp;
import akka.cluster.ClusterEvent.MemberEvent;
import akka.cluster.ClusterEvent.UnreachableMember;
import akka.event.Logging;
import akka.event.LoggingAdapter;

public class ProcessReduce extends AbstractActor implements Serializable{
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
	  }

	  // re-subscribe when restart
	  @Override
	  public void postStop() {
	    cluster.unsubscribe(getSelf());
	  }
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Cache cache;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	private boolean topersist = false;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	int terminatingsize;
	int initialsize = 0;
	DiskSpillingList diskspilllist;
	protected List getFunctions() {
		log.debug("Entered ProcessReduce");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting ProcessReduce");
		return functions;
	}

	private ProcessReduce(JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<ActorSelection> childpipes,
			int terminatingsize) {
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.terminatingsize = terminatingsize;		
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processReduce)
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

	private void processReduce(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			initialsize++;
			if (initialsize == terminatingsize) {
				if (CollectionUtils.isEmpty(childpipes) && object.getValue() instanceof Map filemap) {
					((Map<Integer, String>) filemap).entrySet().stream().forEach(entry -> {
						Stream<Tuple2> datastream = null;
						try {
							datastream = (Stream<Tuple2>) Utils
									.getStreamData(new FileInputStream((String) entry.getValue()));
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);
								var fsdos = new ByteArrayOutputStream();
								var sos = new SnappyOutputStream(fsdos);
								var output = new Output(sos);) {
							Utils.getKryo().writeClassAndObject(output, streammap.collect(Collectors.toList()));
							output.flush();
							tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
							cacheAble(fsdos);
						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					});
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
				} else {
					log.info("Reduce Started");
					diskspilllist = new DiskSpillingList(tasktoprocess, DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
							DataSamudayaConstants.EMPTY, false, false, false, null, null, 0);
					Map<Integer, String> filemap = (Map<Integer, String>) object.getValue();
					final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("left") ? true : false;
					final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("right") ? true : false;
					filemap.entrySet().stream().forEach(entry -> {
						Stream<Tuple2> datastream = null;
						try {
							datastream = (Stream<Tuple2>) Utils
									.getStreamData(new FileInputStream((String) entry.getValue()));
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
						try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);) {
							streammap.forEach(diskspilllist::add);

						} catch (Exception ex) {
							log.error(DataSamudayaConstants.EMPTY, ex);
						}
					});
					diskspilllist.close();
					childpipes.stream().forEach(action -> action
							.tell(new OutputObject(diskspilllist, leftvalue, rightvalue), ActorRef.noSender()));
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
					log.info("Reduce Completed");
				}
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
