package com.github.datasamudaya.stream.executors.actors;

import java.io.ByteArrayOutputStream;
import java.io.FileInputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.ExecutorService;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.ehcache.Cache;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.NodeIndexKey;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TerminatingActorValue;
import com.github.datasamudaya.common.utils.DiskSpillingList;
import com.github.datasamudaya.common.utils.DiskSpillingSet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.PipelineException;
import com.github.datasamudaya.stream.utils.StreamUtils;

import akka.actor.AbstractActor;
import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * Akka actors for the Reduce operators.
 * @author Arun
 *
 */
public class ProcessReduce extends AbstractActor implements Serializable {
	Logger log = LoggerFactory.getLogger(ProcessReduce.class);
	
	protected JobStage jobstage;
	protected FileSystem hdfs;
	protected boolean completed;
	Cache cache;
	Task tasktoprocess;
	boolean iscacheable;
	ExecutorService executor;
	private final boolean topersist = false;
	Map<String, Boolean> jobidstageidtaskidcompletedmap;
	List<ActorSelection> childpipes;
	int terminatingsize;
	int initialshufflesize;
	DiskSpillingList diskspilllist;
	DiskSpillingList diskspilllistinterm;
	int diskspillpercentage;

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
		diskspillpercentage = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT));		
		diskspilllistinterm = new DiskSpillingList(tasktoprocess, diskspillpercentage,
				DataSamudayaConstants.EMPTY, true, false, false, null, null, 0);
	}

	@Override
	public Receive createReceive() {
		return receiveBuilder()
				.match(OutputObject.class, this::processReduce)
				.build();
	}

	private void processReduce(OutputObject object) throws PipelineException, Exception {
		if (Objects.nonNull(object) && Objects.nonNull(object.getValue())) {
			if (object.getValue() instanceof TerminatingActorValue tav) {
				this.terminatingsize = tav.getTerminatingval();
			} else if (object.getValue() instanceof ShuffleBlock sb) {								
				try {
					Object obj = sb.getData();					
					if (obj instanceof DiskSpillingList dsl) {
						if (dsl.isSpilled()) {
							log.info("ProcessReduce::: Spilled Write Started...");
							Utils.copySpilledDataSourceToDestination(dsl, diskspilllistinterm);
							log.info("ProcessReduce::: Spilled Write Completed");
						} else {
							log.info("ProcessReduce::: NotSpilled {}",  dsl.isSpilled());
							diskspilllistinterm.addAll(dsl.getData());
							log.info("ProcessReduce::: NotSpilled Completed {}",  dsl.isSpilled());
						}
						dsl.clear();
					} else if (obj instanceof byte[] data){
						diskspilllistinterm.addAll((Collection<?>)Utils.convertBytesToObjectCompressed(data, null));
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			} 
			if (object.getTerminiatingclass() == DiskSpillingList.class
					|| object.getTerminiatingclass() == Dummy.class
					|| object.getTerminiatingclass() == ShuffleBlock.class
					|| object.getTerminiatingclass() == NodeIndexKey.class
					|| object.getTerminiatingclass() == DiskSpillingSet.class
					|| object.getTerminiatingclass() == TreeSet.class) {
				initialshufflesize++;
				log.info("ProcessReduce::InitialShuffleSize {} , Terminating Size {}", initialshufflesize, terminatingsize);
			}
			if (initialshufflesize == terminatingsize) {
				if(diskspilllistinterm.isSpilled()) {
					diskspilllistinterm.close();
				}
				if (CollectionUtils.isEmpty(childpipes)) {
						Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
							? (Stream<Tuple2>) Utils.getStreamData(
							new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true,
									diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
							: diskspilllistinterm.getData().stream();
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
					jobidstageidtaskidcompletedmap.put(Utils.getIntermediateInputStreamTask(tasktoprocess), true);
				} else {
					log.info("Reduce Started");
					diskspilllist = new DiskSpillingList(tasktoprocess, diskspillpercentage,
							DataSamudayaConstants.EMPTY, false, false, false, null, null, 0);
						try {
							Stream<Tuple2> datastream = diskspilllistinterm.isSpilled()
									? (Stream<Tuple2>) Utils.getStreamData(
									new FileInputStream(Utils.getLocalFilePathForTask(diskspilllistinterm.getTask(), null, true,
											diskspilllistinterm.getLeft(), diskspilllistinterm.getRight())))
									: diskspilllistinterm.getData().stream();
							try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), datastream);) {
								streammap.forEach(diskspilllist::add);

							}
						} catch (Exception e) {
							log.error(DataSamudayaConstants.EMPTY, e);
						}
					
					if(diskspilllist.isSpilled()) {
						diskspilllist.close();
					}
					childpipes.stream().forEach(action -> action
							.tell(new OutputObject(diskspilllist, false, false, DiskSpillingList.class), ActorRef.noSender()));
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
