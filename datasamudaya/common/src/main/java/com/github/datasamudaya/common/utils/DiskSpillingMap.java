package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.File;
import java.io.FileOutputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;

import org.apache.commons.collections.MapUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Output;
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
public class DiskSpillingMap<T, U> implements Map<T, List<U>>, Serializable,AutoCloseable {

	private static final Logger log = LoggerFactory.getLogger(DiskSpillingMap.class);

	private Map<T, List<U>> map;
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
	private String appendwithpath;
	private Kryo kryo;
	private double spillpercentage;

	public DiskSpillingMap() {
		lock = new Semaphore(1);
	}

	public DiskSpillingMap(Task task, String appendwithpath) {
		this.task = task;
		this.appendwithpath = appendwithpath;
		this.kryo = Utils.getKryoInstance();
		diskfilepath = Utils.getLocalFilePathForMRTask(task, appendwithpath);
		map = new ConcurrentHashMap<T, List<U>>();
		this.keys = new LinkedHashSet<>();
		this.spillpercentage = (Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT))) / 100.0;
		this.lock = new Semaphore(1);
		this.batchsize = Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DISKSPILLDOWNSTREAMBATCHSIZE,
				DataSamudayaConstants.DISKSPILLDOWNSTREAMBATCHSIZE_DEFAULT));
		this.isclosed = false;
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
	 * The method which returns the task of the spilled data to disk
	 * @return task
	 */
	public Task getTask() {
		return this.task;
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
			lock.acquire();
			if ((isspilled 
					|| Utils.isMemoryUsageLimitExceedsGraphLayoutSize(map, spillpercentage)
					|| Utils.isMemoryUsageHigh(spillpercentage))
					&& map.size() > 0) {
				if (isNull(ostream)) {
					ostream = new FileOutputStream(new File(diskfilepath), true);
					sos = new SnappyOutputStream(ostream);
					op = new Output(sos);
					isspilled = true;
				}
				if (isspilled && (map.values().size() >= batchsize) || isfstoclose && map.values().size() > 0) {
					kryo.writeClassAndObject(op, map);
					op.flush();
					map.clear();
				}
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
				if (map.values().size() > 0) {
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
			}
			this.isclosed = true;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

	@Override
	public boolean isEmpty() {
		return MapUtils.isEmpty(map);
	}

	@Override
	public boolean containsKey(Object key) {		
		return keys.contains(key);
	}

	@Override
	public boolean containsValue(Object value) {
		return map.containsValue(value);
	}

	@Override
	public List<U> remove(Object key) {		
		return (List<U>) map.remove(key);
	}

	@Override
	public int size() {		
		return keys.size();
	}

	@Override
	public List<U> get(Object key) {		
		return (List<U>) map.get(key);
	}

	@Override
	public void clear() {
		map.clear();
	}

	@Override
	public List<U> put(T key, List<U> value) {
		keys.add(key);
		return map.put(key, value);
	}

	@Override
	public void putAll(Map<? extends T, ? extends List<U>> m) {
		keys.addAll(m.keySet());
		map.putAll(map);
	}

	@Override
	public Set<T> keySet() {		
		return map.keySet();
	}

	@Override
	public Collection<List<U>> values() {
		return map.values();
	}

	@Override
	public Set<Entry<T, List<U>>> entrySet() {
		return map.entrySet();
	}

}
