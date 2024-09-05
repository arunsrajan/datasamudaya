package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.Semaphore;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.io.Output;
import com.github.datasamudaya.common.Context;
import com.github.datasamudaya.common.DataCruncherContext;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Task;

/**
 * The class which spills the Map Reduce data to disk when the memory exceeds the
 * percentage
 * 
 * @author arun
 *
 */
public class DiskSpillingContext<T, U> implements Context<T, U>, Serializable,AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingContext.class);

	private DataCruncherContext<T, U> context;
	private byte[] bytes;
	private Set keys;
	private String diskfilepath;
	private boolean isspilled;
	private boolean isclosed;
	private int batchsize;
	private Task task;
	private transient OutputStream ostream;
	private transient Output op;
	private transient SnappyOutputStream sos;
	private Semaphore lock;
	private Semaphore filelock;
	private String appendwithpath;
	public DiskSpillingContext() {
		lock = new Semaphore(1);
		filelock = new Semaphore(1);
	}

	public DiskSpillingContext(Task task, String appendwithpath) {
		this.task = task;
		this.appendwithpath = appendwithpath;		
		diskfilepath = Utils.getLocalFilePathForMRTask(task, appendwithpath);
		context = new DataCruncherContext<T, U>();
		this.keys = new HashSet();
		Utils.mpBeanLocalToJVM.setUsageThreshold((long) Math.floor(Utils.mpBeanLocalToJVM.getUsage().getMax() * ((Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE, 
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT))) / 100.0)));
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
	public DataCruncherContext<T, U> getData() {
		return context;
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
	 * The function returns append string with path
	 * @return returns append string with path
	 */
	public String getAppendwithpath() {
		return appendwithpath;
	}

	/**
	 * The method adds the value to the list and spills to disk when memory
	 * exceeds limit
	 * 
	 * @param value
	 * @return 
	 */
	@Override
	public void put(T key, U value) {		
		spillToDiskIntermediate(false);
		context.put(key, value);	
		keys.add(key);
	}

	/**
	 * The method adds all the data from list to the target list
	 * 
	 * @param value
	 */
	public void addKeyCollection(T key, Collection<U> values) {
		if (nonNull(key)&&CollectionUtils.isNotEmpty(values)) {
			keys.add(key);
			values.parallelStream().forEach(value->put(key, value));
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
	public DataCruncherContext<T, U> readContextFromBytes() {
		if (nonNull(bytes)) {
			return (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
		}
		return new DataCruncherContext<>();
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
					&& context.valuesSize()>0) {
				filelock.acquire();
				if (isNull(ostream)) {					
					ostream = new FileOutputStream(new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new Output(sos);
					isspilled = true;
				}
				filelock.release();
				lock.acquire();
				if (isspilled && (context.valuesSize() >= batchsize) || isfstoclose && context.valuesSize()>0) {					
					Utils.getKryoInstance().writeClassAndObject(op, context);
					op.flush();
					context.clear();
				}
			}

		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {
			if ((isspilled || Utils.mpBeanLocalToJVM.isUsageThresholdExceeded()) 
					&& context.valuesSize()>0) {
				lock.release();
			}
		}
	}


	@Override
	public void close() throws Exception {
		try {
			if (isspilled) {
				if (context.valuesSize() > 0) {
					spillToDiskIntermediate(true);
				}
				log.info("Closing Stream For Task {} {} {} {}", task, op, sos, ostream);
				if (nonNull(op)) {
					op.close();
				}
				if (nonNull(sos)) {
					sos.close();
				}
				if(nonNull(ostream)) {
					ostream.close();
				}
				op = null;
				sos = null;
				ostream = null;
			} else {
				if (isNull(bytes)) {
					bytes = Utils.convertObjectToBytesCompressed(context, null);
					context = null;
				}
			}
			this.isclosed = true;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	@Override
	public void clear() {
		if (nonNull(bytes)) {
			bytes = null;
		} else if (context.valuesSize()>0) {
			context.clear();
			context = null;
		}
	}
	
	@Override
	public long size() {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			return (int) (isNull(context) ? 0 : context.valuesSize());
		} else if(nonNull(context)){
			return (int) (isNull(context) ? 0 : context.valuesSize());
		}
		throw new UnsupportedOperationException();
	}

	@Override
	public Collection<U> get(T key) {
		if(nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			return context.get(key);
		} else if(nonNull(context)) {
			return context.get(key);
		}
		throw new UnsupportedOperationException();
	}
	
	@Override
	public Set<Entry<T, Collection<U>>> entries() {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			return context.entries();
		} else if(nonNull(context)) {
			return context.entries();
		}
		throw new UnsupportedOperationException();
	}

	public void putCollectionValue(Set<T> k, U v) {
		k.stream().forEach(key -> {
			put(key, v);
		});
	}
	
	@Override
	public void putAll(Set<T> k, U v) {		
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			putCollectionValue(k, v);			
		} else if(nonNull(context)) {
			putCollectionValue(k, v);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public Set<T> keys() {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			return context.keys();
		} else if(!isspilled && nonNull(context)) {
			return context.keys();
		} else {
			return new LinkedHashSet<>(keys);
		}
	}

	@Override
	public void addAll(T k, Collection<U> v) {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			addKeyCollection(k, v);
		} else if(nonNull(context)) {
			addKeyCollection(k, v);
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public void add(Context<T, U> ctx) {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			ctx.keys().stream().forEach(key -> addAll(key, ctx.get(key)));
		} else if(nonNull(context)) {
			ctx.keys().stream().forEach(key -> addAll(key, ctx.get(key)));
		} else {
			throw new UnsupportedOperationException();
		}
	}

	@Override
	public long valuesSize() {
		if (nonNull(bytes) && isNull(context)) {
			context = (DataCruncherContext<T, U>) Utils.convertBytesToObjectCompressed(bytes, null);
			return context.valuesSize();
		} else if(nonNull(context)) {
			return context.valuesSize();
		} else {
			throw new UnsupportedOperationException();
		}
	}

}
