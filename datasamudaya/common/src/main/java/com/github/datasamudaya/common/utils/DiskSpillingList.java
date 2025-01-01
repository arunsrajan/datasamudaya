package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.AbstractList;
import java.util.Vector;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

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

import akka.actor.ActorRef;
import akka.actor.ActorSelection;
import akka.cluster.sharding.typed.javadsl.EntityRef;

/**
 * The class which spills the data to disk when the memory exceeds the
 * percentage
 * 
 * @author arun
 *
 */
public class DiskSpillingList<T> extends AbstractList<T> implements Serializable,AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingList.class);

	private List dataList;
	private byte[] bytes;
	private String diskfilepath;
	private boolean isspilled;
	private boolean isclosed;
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
	private Map<Integer, EntityRef> downstreampipelines;
	private Map<Integer, FilePartitionId> filepartids;
	int numfileperexec;
	private Semaphore lock;
	private Semaphore filelock;

	public DiskSpillingList() {
		lock = new Semaphore(1);
		filelock = new Semaphore(1);
	}

	public DiskSpillingList(Task task, int spillexceedpercentage, String appendwithpath, boolean appendintermediate, boolean left, boolean right, Map<Integer, FilePartitionId> filepartids, Map<Integer, EntityRef> downstreampipelines, int numfileperexec) {
		this.task = task;
		diskfilepath = Utils.getLocalFilePathForTask(task, appendwithpath, appendintermediate, left, right);
		dataList = new Vector<>();
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
		this.isclosed = false;
	}

	/**
	 * The method returns data list object
	 * 
	 * @return datalist object
	 */
	public List<T> getData() {
		return dataList;
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
	 * The function returns whether the stream is closed or not
	 * @return is stream closed
	 */
	public boolean isClosed() {
		return isclosed;
	}

	/**
	 * The method adds the value to the list and spills to disk when memory
	 * exceeds limit
	 * 
	 * @param value
	 * @return 
	 */
	@Override
	public boolean add(T value) {
		if (isNull(dataList)) {
			dataList = new Vector<>();
		}
		spillToDiskIntermediate(false);
		dataList.add(value);
		return true;
	}

	/**
	 * The method adds all the data from list to the target list
	 * 
	 * @param value
	 */
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
	 * The function returns List into compressed bytes
	 * @return compressed bytes
	 */
	public byte[] getBytes() {
		return bytes;
	}

	/**
	 * The function reads compresseed bytes to List 
	 * @return
	 */
	public List readListFromBytes() {
		if (nonNull(bytes)) {
			return (List) Utils.convertBytesToObjectCompressed(bytes, null);
		}
		return new Vector<>();
	}

	/**
	 * The function returns local disk file path when list is spilled to disk 
	 * @return
	 */
	public String getDiskfilepath() {
		return this.diskfilepath;
	}

	protected void spillToDiskIntermediate(boolean isfstoclose) {
		try {
			if ((isspilled || Utils.mpBeanLocalToJVM.isUsageThresholdExceeded())
					&& CollectionUtils.isNotEmpty(dataList)) {
				filelock.acquire();
				if (isNull(ostream)) {
					ostream = new FileOutputStream(new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new Output(sos);
					isspilled = true;
				}
				filelock.release();
				lock.acquire();
				if (isspilled && (dataList.size() >= batchsize) || isfstoclose && CollectionUtils.isNotEmpty(dataList)) {
					Utils.getKryoInstance().writeClassAndObject(op, dataList);
					op.flush();
					dataList.clear();
				}
			} else if (nonNull(downstreampipelines) && dataList.size() >= batchsize || isfstoclose && CollectionUtils.isNotEmpty(dataList)) {
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
				if (CollectionUtils.isNotEmpty(dataList)) {
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
			} else if (nonNull(downstreampipelines) && CollectionUtils.isNotEmpty(dataList)) {
				transferDataToDownStreamPipelines();
			} else {
				if (isNull(bytes)) {
					bytes = Utils.convertObjectToBytesCompressed(dataList, null);
					dataList.clear();
				}
			}
			this.isclosed = true;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	/**
	 * The method transfers data list to downstream pipelines;
	 */
	protected void transferDataToDownStreamPipelines() {
		Object obj = dataList.get(0);
		int filetransferindex;
		if (obj instanceof Tuple2 tup2) {
			filetransferindex = Math.abs(tup2.v1.hashCode()) % numfileperexec;
		} else {
			filetransferindex = Math.abs(obj.hashCode()) % numfileperexec;
		}
		EntityRef actorselection = downstreampipelines.get(filetransferindex);
		actorselection.tell(new OutputObject(new ShuffleBlock(null,
						Utils.convertObjectToBytes(filepartids.get(filetransferindex)), Utils.convertObjectToBytesCompressed(dataList, null)), left, right, null));
		dataList.clear();
	}

	@Override
	public void clear() {
		if (nonNull(bytes)) {
			bytes = null;
		} else if (CollectionUtils.isNotEmpty(dataList)) {
			dataList.clear();
			dataList = null;
		}
	}

	@Override
	public int size() {
		if (nonNull(bytes)) {
			dataList = (List) Utils.convertBytesToObjectCompressed(bytes, null);
		}
		return isNull(dataList) ? 0 : dataList.size();
	}

	@Override
	public T get(int index) {
		return (T) (isNull(dataList) ? null : dataList.get(index));
	}

}
