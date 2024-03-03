package com.github.datasamudaya.stream.executors.actors;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayOutputStream;
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
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.Task;
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

	public static record BlocksLocationRecord(FileSystem hdfs, List<ActorSelection> pipeline) implements Serializable {
	}

	java.util.List<Object> result = new java.util.Vector<>();
	java.util.List<Object> resultcollector = new java.util.Vector<>();

	@Override
	public Receive createReceive() {
		return receiveBuilder().match(OutputObject.class, this::processBlocksLocationRecord).build();
	}

	private void processBlocksLocationRecord(OutputObject object) throws PipelineException {
		if (Objects.nonNull(object) && Objects.nonNull(object.value())) {
			initialsize++;
			if (object.value() instanceof List values) {
				result.addAll(values);
			} else {
				result.add(object.value());
			}
			if (initialsize == terminatingsize) {
				if (CollectionUtils.isEmpty(childpipes)) {
					try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), result.stream());
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
					try (var streammap = (Stream) StreamUtils.getFunctionsToStream(getFunctions(), result.stream());) {
						List out = (List) streammap.collect(Collectors.toList());
						childpipes.parallelStream().forEach(
								action -> action.tell(new OutputObject(out, leftvalue, rightvalue), ActorRef.noSender()));
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
