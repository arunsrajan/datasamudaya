package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.util.List;
import java.util.Vector;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.Task;

/**
 * The class which spills the data to disk when the memory exceeds the
 * percentage
 * 
 * @author arun
 *
 */
public class DiskSpillingList<T> implements AutoCloseable{

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingList.class);
	
	private List dataList;
	private long totalmemoryavailable;
	private Runtime rt;
	private String diskfilepath;
	private boolean isspilled = false;
	private int batchsize = 5000;
	private Task task;
	private Semaphore lock;
	private boolean left;
	private boolean right;
	private boolean appendintermediate;
	private FileOutputStream ostream;
	private com.esotericsoftware.kryo.io.Output op;
	private Kryo kryo;
	private SnappyOutputStream sos;
	public DiskSpillingList(Task task, int spillexceedpercentage, boolean appendintermediate, boolean left, boolean right) {
		this.task = task;
		diskfilepath = Utils.getLocalFilePathForTask(task, appendintermediate, left, right);
		dataList = new Vector<>();
		rt = Runtime.getRuntime();
		totalmemoryavailable = rt.maxMemory() * spillexceedpercentage / 100;
		lock = new Semaphore(1);
		this.left = left;
		this.right = right;
		this.appendintermediate = appendintermediate;
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
	 * The method adds the value to the list and spills to disk when memory
	 * exceeds limit
	 * 
	 * @param value
	 */
	public void add(T value) {
		dataList.add(value);
		spillToDiskIntermediate();
	}

	/**
	 * The method adds all the data from list to the target list
	 * 
	 * @param value
	 */
	public void addAll(List<T> values) {
		values.stream().forEach(this::add);
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
	
	protected void spillToDiskIntermediate() {
		try {
			lock.acquire();
			if ((rt.totalMemory() > totalmemoryavailable
					|| isspilled) && CollectionUtils.isNotEmpty(dataList)) {
				if(isNull(ostream)) {
					isspilled = true;
					ostream = new FileOutputStream( new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new com.esotericsoftware.kryo.io.Output(
							sos);
					kryo = Utils.getKryo();
				}
				if(rt.totalMemory() > totalmemoryavailable || dataList.size()>=batchsize) {
					kryo.writeClassAndObject(op, new Vector<>(dataList));
					op.flush();
					dataList.clear();
				}
			}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {
			lock.release();
		}
	}


	@Override
	public void close() throws Exception {	
		try {
			if(nonNull(ostream)) {
				if(CollectionUtils.isNotEmpty(dataList)) {
					batchsize = dataList.size();
					spillToDiskIntermediate();
				}
				if(nonNull(op)) {
					op.close();
				}
				if(nonNull(sos)) {
					sos.close();
				}
				ostream.close();
				op = null;
				sos = null;
				ostream = null;
			}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

}
