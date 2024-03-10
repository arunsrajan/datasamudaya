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

import org.apache.commons.collections.MapUtils;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.StreamUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

public class ProcessMapperByStream extends AbstractActor {
	protected JobStage jobstage;
	private static org.slf4j.Logger log = LoggerFactory.getLogger(ProcessMapperByStream.class);
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
	DiskSpillingList diskspilllistinterm;
	DiskSpillingList diskspilllist;
	Map<Integer, FilePartitionId> shufflerectowrite;
	protected List getFunctions() {
		log.debug("Entered ProcessMapperByStream");
		var tasks = jobstage.getStage().tasks;
		var functions = new ArrayList<>();
		for (var task : tasks) {
			functions.add(task);
		}
		log.debug("Exiting ProcessMapperByStream");
		return functions;
	}

	private ProcessMapperByStream(JobStage js, FileSystem hdfs, Cache cache,
			Map<String, Boolean> jobidstageidtaskidcompletedmap, Task tasktoprocess, List<ActorSelection> childpipes,
			Map<Integer, FilePartitionId> shufflerectowrite,
			int terminatingsize) {
		this.jobstage = js;
		this.hdfs = hdfs;
		this.cache = cache;
		this.jobidstageidtaskidcompletedmap = jobidstageidtaskidcompletedmap;
		this.iscacheable = true;
		this.tasktoprocess = tasktoprocess;
		this.childpipes = childpipes;
		this.shufflerectowrite = shufflerectowrite;
		this.terminatingsize = terminatingsize;
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, true, false, false);
		diskspilllist = new DiskSpillingList(tasktoprocess, DataSamudayaConstants.SPILLTODISK_PERCENTAGE, null, false, false, false);
	}

	public static record BlocksLocationRecord(FileSystem hdfs, List<ActorSelection> pipeline) implements Serializable {
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processMapperByStream).build();
	}

	private void processMapperByStream(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.value())) {
			initialsize++;
			if(object.value() instanceof DiskSpillingList dsl) {
				if(dsl.isSpilled()) {
					Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
				} else {
					diskspilllistinterm.addAll(dsl.getData());
				}
			}
			if (initialsize == terminatingsize) {
				if (CollectionUtils.isEmpty(childpipes)) {
					Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()?(Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.
							getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true, diskspilllistinterm.getLeft(), diskspilllistinterm.getRight()))):diskspilllistinterm.getData().stream();
					try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);
							var fsdos = new ByteArrayOutputStream();
							var sos = new SnappyOutputStream(fsdos);
							var output = new Output(sos);) {
						log.info("Map assembly deriving");
						Utils.getKryo().writeClassAndObject(output, streammap.collect(Collectors.toList()));
						output.flush();
						tasktoprocess.setNumbytesgenerated(fsdos.toByteArray().length);
						cacheAble(fsdos);
						log.info("Map assembly concluded");
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
				} else {
					final boolean leftvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("left") ? true : false;
					final boolean rightvalue = isNull(tasktoprocess.joinpos) ? false
							: nonNull(tasktoprocess.joinpos) && tasktoprocess.joinpos.equals("right") ? true : false;
					Stream datastream = diskspilllistinterm.isSpilled()?(Stream<Tuple2>) Utils.getStreamData(new FileInputStream(Utils.
							getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true, false, false))):diskspilllistinterm.getData().stream();
					try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);) {						
						if (MapUtils.isNotEmpty(shufflerectowrite)) {
							int numberoffilepartitions = shufflerectowrite.size();
							Map<Integer, DiskSpillingList> results = ((Stream<Tuple2>) streammap).collect(
									Collectors.groupingByConcurrent(tup2 -> tup2.v1.hashCode() % numberoffilepartitions,
											Collectors.mapping(tup2 -> tup2,
													Collectors.toCollection(() -> new DiskSpillingList(tasktoprocess,
															DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
															Utils.getUUID().toString(), false, leftvalue, rightvalue)))));
							results.entrySet().forEach(entry -> {
								try {
									entry.getValue().close();
								} catch (Exception ex) {
									log.error(DataSamudayaConstants.EMPTY, ex);
								}
								shufflerectowrite.get(entry.getKey()).getActorSelection()
										.tell(new OutputObject(new ShuffleBlock(null,
												shufflerectowrite.get(entry.getKey()), entry.getValue()), leftvalue, rightvalue),
												ActorRef.noSender());
							});
							shufflerectowrite.entrySet().stream().forEach(entry -> entry.getValue().getActorSelection()
									.tell(new OutputObject(new Dummy(), leftvalue, rightvalue), ActorRef.noSender()));
						} else {
							streammap.forEach(diskspilllist::add);
							diskspilllist.close();
							childpipes.parallelStream().forEach(
								action -> action.tell(new OutputObject(diskspilllist, leftvalue, rightvalue), ActorRef.noSender()));
						}
					} catch (Exception ex) {
						log.error(DataSamudayaConstants.EMPTY, ex);
					}
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
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
