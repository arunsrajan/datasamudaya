package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.FilePartitionId;
import com.github.datasamudaya.common.OutputObject;
import com.github.datasamudaya.common.ShuffleBlock;
import com.github.datasamudaya.common.Task;

import akka.actor.ActorRef;
import akka.actor.ActorSelection;

/**
 * The class which spills the data to disk when the memory exceeds the
 * percentage
 * 
 * @author arun
 *
 */
public class DiskSpillingList<T> extends AbstractList<T> implements AutoCloseable{

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingList.class);
	
	private List dataList;
	private byte[] bytes;
	private long totalmemoryavailable;
	private Runtime rt;
	private String diskfilepath;
	private boolean isspilled = false;
	private int batchsize = 2000;
	private Task task;
	private Semaphore lock;
	private boolean left;
	private boolean right;
	private boolean appendintermediate;
	private OutputStream ostream;
	private com.esotericsoftware.kryo.io.Output op;
	private Kryo kryo;
	private SnappyOutputStream sos;
	private String appendwithpath;
	Map<Integer, ActorSelection> downstreampipelines;
	Map<Integer, FilePartitionId> filepartids;
	int numfileperexec;
	public DiskSpillingList(Task task, int spillexceedpercentage, String appendwithpath, boolean appendintermediate, boolean left, boolean right,Map<Integer, FilePartitionId> filepartids, Map<Integer, ActorSelection> downstreampipelines,int numfileperexec) {
		this.task = task;
		diskfilepath = Utils.getLocalFilePathForTask(task, appendwithpath, appendintermediate, left, right);
		dataList = new ArrayList<>();
		rt = Runtime.getRuntime();
		totalmemoryavailable = rt.maxMemory() * spillexceedpercentage / 100;
		lock = new Semaphore(1);
		this.left = left;
		this.right = right;
		this.appendintermediate = appendintermediate;
		this.appendwithpath = appendwithpath;
		this.downstreampipelines = downstreampipelines;
		this.filepartids = filepartids;
		this.numfileperexec = numfileperexec;
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
	 * @return 
	 */
	@Override
	public boolean add(T value) {
		dataList.add(value);
		spillToDiskIntermediate(false);
		return true;
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
		try (var istream = new ByteArrayInputStream(bytes);
				var sis = new SnappyInputStream(istream);
				var ip = new com.esotericsoftware.kryo.io.Input(sis);) {
			return (List) Utils.getKryo().readClassAndObject(ip);
		} catch (Exception e) {
			log.error(DataSamudayaConstants.EMPTY, e);
		}
		return null;
	}
	
	protected void spillToDiskIntermediate(boolean isfstoclose) {
		try {
			if ((rt.freeMemory() < totalmemoryavailable
					|| isspilled) && CollectionUtils.isNotEmpty(dataList)) {
				if (isNull(ostream)) {
					isspilled = true;
					ostream = new FileOutputStream(new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new com.esotericsoftware.kryo.io.Output(sos);
					kryo = Utils.getKryo();
				}
				if (rt.freeMemory() < totalmemoryavailable && (dataList.size() >= batchsize || isfstoclose && CollectionUtils.isNotEmpty(dataList))) {
					kryo.writeClassAndObject(op, dataList);
					op.flush();
					dataList.clear();
				}
			} else if(nonNull(downstreampipelines) && dataList.size()>=batchsize) {
				transferDataToDownStreamPipelines();
			}
			
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {

		}
	}


	@Override
	public void close() throws Exception {	
		try {
			if(nonNull(ostream)) {
				if(CollectionUtils.isNotEmpty(dataList)) {
					spillToDiskIntermediate(true);
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
			} else if(nonNull(downstreampipelines) && CollectionUtils.isNotEmpty(dataList)) {
				transferDataToDownStreamPipelines();
			} else {
				try (var ostream = new ByteArrayOutputStream();
						var sos = new SnappyOutputStream(ostream);
						var op = new com.esotericsoftware.kryo.io.Output(sos);) {
					kryo = Utils.getKryo();
					kryo.writeClassAndObject(op, dataList);
					op.flush();
					bytes= ostream.toByteArray();
					dataList.clear();
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			}
		} catch(Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}
	
	/**
	 * The method transfers data list to downstream pipelines;
	 */
	protected void transferDataToDownStreamPipelines() {
		Object obj = dataList.get(0);
		int filetransferindex;
		if(obj instanceof Tuple2 tup2) {
			filetransferindex = Math.abs(tup2.v1.hashCode()) % numfileperexec;
		} else {
			filetransferindex = Math.abs(obj.hashCode()) % numfileperexec;
		}
		ActorSelection actorselection = downstreampipelines.get(filetransferindex);
		actorselection.tell(new OutputObject(new ShuffleBlock(null,
				Utils.convertObjectToBytes(filepartids.get(filetransferindex)), Utils.convertObjectToBytes(dataList)), left, right),
				ActorRef.noSender());
		dataList.clear();
	}

	@Override
	public int size() {		
		return dataList.size();
	}

	@Override
	public T get(int index) {		
		return (T) dataList.get(index);
	}

}
