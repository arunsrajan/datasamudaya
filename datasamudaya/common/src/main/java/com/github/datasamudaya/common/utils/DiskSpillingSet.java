package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.AbstractSet;
import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;
import java.util.stream.Stream;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Dummy;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.functions.SortedComparator;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * The class which spills the data to disk when the memory exceeds the
 * percentage
 * 
 * @author arun
 *
 */
public class DiskSpillingSet<T> extends AbstractSet<T> implements Serializable,AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingSet.class);

	private Set dataSet;
	private byte[] bytes;
	private String diskfilepath;
	private boolean isspilled;
	private int batchsize;
	private Task task;
	private boolean left;
	private boolean right;
	private boolean appendintermediate;
	private transient OutputStream ostream;
	private transient Output op;
	private transient Kryo kryo;
	private transient SnappyOutputStream sos;
	private String appendwithpath;
	private Map<Integer, ActorSelection> downstreampipelines;
	private Map<Integer, FilePartitionId> filepartids;
	int numfileperexec;
	private Semaphore lock;
	private Semaphore filelock;
	private boolean istree;

	public DiskSpillingSet() {
		lock = new Semaphore(1);
		filelock = new Semaphore(1);
	}

	public DiskSpillingSet(Task task, int spillexceedpercentage, 
			String appendwithpath, boolean appendintermediate, 
			boolean left, boolean right, Map<Integer, FilePartitionId> filepartids, 
			Map<Integer, ActorSelection> downstreampipelines, int numfileperexec, boolean istree
			,SortedComparator sortedcomparator) {
		this.task = task;
		diskfilepath = Utils.getLocalFilePathForTask(task, appendwithpath, appendintermediate, left, right);
		dataSet = istree?new TreeSet<>(sortedcomparator):new HashSet();
		Utils.mpBeanLocalToJVM.setUsageThreshold((long) Math.floor(Utils.mpBeanLocalToJVM.getUsage().getMax() * (spillexceedpercentage / 100.0)));
		this.left = left;
		this.right = right;
		this.appendintermediate = appendintermediate;
		this.appendwithpath = appendwithpath;
		this.downstreampipelines = downstreampipelines;
		this.filepartids = filepartids;
		this.numfileperexec = numfileperexec;
		this.lock = new Semaphore(1);
		this.filelock = new Semaphore(1);
		this.batchsize = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DISKSPILLDOWNSTREAMBATCHSIZE,
				DataSamudayaConstants.DISKSPILLDOWNSTREAMBATCHSIZE_DEFAULT));
	}

	/**
	 * The method returns data Set object
	 * 
	 * @return dataSet object
	 */
	public Set<T> getData() {
		return dataSet;
	}

	/**
	 * The method returns whether data got spilled to disk or it is inmemory.
	 * 
	 * @return spilled or not
	 */
	public boolean isSpilled() {
		return isspilled;
	}

	/**
	 * The method adds the value to the Set and spills to disk when memory
	 * exceeds limit
	 * 
	 * @param value
	 * @return 
	 */
	@Override
	public boolean add(T value) {
		if (isNull(dataSet)) {
			dataSet = istree?new TreeSet<>(new ObjectArrayComparator()):new HashSet();
		}
		spillToDiskIntermediate(false);
		dataSet.add(value);
		return true;
	}

	/**
	 * The method adds all the data from Set to the target Set
	 * 
	 * @param value
	 */
	public void addAll(Set<T> values) {
		if (CollectionUtils.isNotEmpty(values)) {
			values.stream().forEach(this::add);
		}
	}

	public void addAll(List<T> values) {
		if (CollectionUtils.isNotEmpty(values)) {
			values.stream().forEach(this::add);
		}
	}

	/**
	 * The method which returns the task of the spilled data to disk
	 * @return task
	 */
	public Task getTask() {
		return this.task;
	}

	/**
	 * The method which returns whether the left value of join is available
	 * @return boolean value
	 */
	public boolean getLeft() {
		return this.left;
	}

	/**
	 * The method which returns whether the values are intermediate data
	 * @return boolean value
	 */
	public boolean getAppendintermediate() {
		return this.appendintermediate;
	}

	/**
	 * The method which returns whether the right value of join is available
	 * @return boolean value
	 */
	public boolean getRight() {
		return this.right;
	}

	/**
	 * The function returns subpath to append with actual path
	 * @return subpath
	 */
	public String getAppendwithpath() {
		return appendwithpath;
	}

	/**
	 * The function returns Set into compressed bytes
	 * @return compressed bytes
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * The function reads compresseed bytes to Set 
	 * @return
	 */
	public Set<T> readSetFromBytes() {
		if (nonNull(bytes)) {
			return (Set) Utils.convertBytesToObjectCompressed(bytes, null);
		}
		return new HashSet();
	}

	/**
	 * The function returns local disk file path when Set is spilled to disk 
	 * @return
	 */
	public String getDiskfilepath() {
		return this.diskfilepath;
	}

	protected void spillToDiskIntermediate(boolean isfstoclose) {
		try {
			if ((isspilled || Utils.mpBeanLocalToJVM.isUsageThresholdExceeded())
					&& CollectionUtils.isNotEmpty(dataSet)) {
				filelock.acquire();
				if (isNull(ostream)) {
					ostream = new FileOutputStream(new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new Output(sos);
					isspilled = true;
				}
				filelock.release();
				lock.acquire();
				if (isspilled && (dataSet.size() >= batchsize) || isfstoclose && CollectionUtils.isNotEmpty(dataSet)) {
					Utils.getKryoInstance().writeClassAndObject(op, dataSet);
					op.flush();
					dataSet.clear();
				}
			} else if (nonNull(downstreampipelines) && dataSet.size() >= batchsize || isfstoclose && CollectionUtils.isNotEmpty(dataSet)) {
				lock.acquire();
				transferDataToDownStreamPipelines();
			}

		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {
			lock.release();
		}
	}


	@Override
	public void close() throws Exception {
		try {
			if (isspilled) {
				if (CollectionUtils.isNotEmpty(dataSet)) {
					spillToDiskIntermediate(true);
				}
				log.debug("Closing Stream For Task {} {} {} {}", task, op, sos, ostream);
				if (nonNull(op)) {
					op.close();
				}
				if (nonNull(sos)) {
					sos.close();
				}
				if (nonNull(ostream)) {
					ostream.close();
				}
				op = null;
				sos = null;
				ostream = null;
			} else if (nonNull(downstreampipelines) && CollectionUtils.isNotEmpty(dataSet)) {
				transferDataToDownStreamPipelines();
			} else {
				if (isNull(bytes)) {
					bytes = Utils.convertObjectToBytesCompressed(dataSet, null);
					dataSet.clear();
				}
			}
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * The method transfers data Set to downstream pipelines;
	 */
	protected void transferDataToDownStreamPipelines() {
		Object obj = dataSet.iterator().next();
		int filetransferindex;
		if (obj instanceof Tuple2 tup2) {
			filetransferindex = Math.abs(tup2.v1.hashCode()) % numfileperexec;
		} else {
			filetransferindex = Math.abs(obj.hashCode()) % numfileperexec;
		}
		ActorSelection actorselection = downstreampipelines.get(filetransferindex);
		actorselection.tell(new OutputObject(new ShuffleBlock(null,
						Utils.convertObjectToBytes(filepartids.get(filetransferindex)), Utils.convertObjectToBytesCompressed(dataSet, null)), left, right, null),
				ActorRef.noSender());
		dataSet.clear();
	}

	@Override
	public void clear() {
		if (nonNull(bytes)) {
			bytes = null;
		} else if (CollectionUtils.isNotEmpty(dataSet)) {
			dataSet.clear();
			dataSet = null;
		}
	}

	@Override
	public int size() {
		if (nonNull(bytes)) {
			dataSet = (Set) Utils.convertBytesToObjectCompressed(bytes, null);
		}
		return isNull(dataSet) ? 0 : dataSet.size();
	}

	@Override
	public Iterator<T> iterator() {
		return dataSet.iterator();
	}
	
	@Override
	public Stream<T> stream() {
		return dataSet.stream();
	}

	@Override
	public boolean addAll(Collection<? extends T> c) {
		addAll((Set<T>) c);
		return true;
	}

}
