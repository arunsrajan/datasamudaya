package com.github.datasamudaya.common.utils;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.xerial.snappy.SnappyInputStream;
import org.xerial.snappy.SnappyOutputStream;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
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
public class DiskSpillingMap<K, V> implements Map<K, V>, Serializable, AutoCloseable {
	
	private static final Logger log = LoggerFactory.getLogger(DiskSpillingMap.class);
    private static final long serialVersionUID = 1L;
    private Map<K, V> inMemoryMap;
    private Map<K, String> keyIndexMap; // Maps keys to file paths
    private File indexFile;
    private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock();
	private double spillpercentage;
	private boolean isspilled;
	private String appendwithpath;
	private Task task;
	private String diskfilepathindex;
	private boolean isclosed;
	private Kryo kryo;
	
    public DiskSpillingMap(Task task, String appendtopath) {
    	kryo = Utils.getKryoInstance();
    	this.spillpercentage = (Integer.valueOf(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.SPILLTODISK_PERCENTAGE,
				DataSamudayaConstants.SPILLTODISK_PERCENTAGE_DEFAULT))) / 100.0;
    	this.appendwithpath = appendtopath;
    	diskfilepathindex = Utils.getLocalFilePathForTaskJoin(task, "diskSpillIndex.idx", false, false, false);
    	this.task = task;
        inMemoryMap = new HashMap<>();
        keyIndexMap = new HashMap<>();
        indexFile = new File(diskfilepathindex);
		loadIndex(); // Load existing index from disk       
    }

    @Override
    public int size() {
        lock.readLock().lock();
        try {
            return inMemoryMap.size() + keyIndexMap.size();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean isEmpty() {
        lock.readLock().lock();
        try {
            return inMemoryMap.isEmpty() && keyIndexMap.isEmpty();
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsKey(Object key) {
        lock.readLock().lock();
        try {
            return inMemoryMap.containsKey(key) || keyIndexMap.containsKey(key);
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public boolean containsValue(Object value) {
        lock.readLock().lock();
        try {
            if (inMemoryMap.containsValue(value)) {
                return true;
            }
            for (K key : keyIndexMap.keySet()) {
                if (get(key).equals(value)) {
                    return true;
                }
            }
            return false;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public V get(Object key) {
        lock.readLock().lock();
        try {
            if (inMemoryMap.containsKey(key)) {
                return inMemoryMap.get(key);
            } else if (keyIndexMap.containsKey(key)) {
                String filePath = keyIndexMap.get(key);
                return readFromDisk(filePath);
            }
            return null;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public V put(K key, V value) {
        lock.writeLock().lock();
        try {
            if ((isspilled 
					|| Utils.
					isMemoryUsageLimitExceedsGraphLayoutSize(inMemoryMap, spillpercentage))					
					&& inMemoryMap.size() > 0) {
                spillToDisk(key, value);
                return null;
            } else {
            	if(value instanceof List && inMemoryMap.containsKey(key)) {
            		List currentlist = (List) inMemoryMap.get(key);
					currentlist.addAll((List) value);
					return inMemoryMap.put(key, (V) currentlist);
				} else {
            		return inMemoryMap.put(key, value);
            	}
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    private void spillToDisk(K key, V value) {
    	try {
    		isspilled = true;
	        String filePath = createDataFile(key);
	        writeToDisk(filePath, value);
	        keyIndexMap.put(key, filePath);
	        saveIndex(); // Update the index file
    	} catch (IOException e) {
			throw new RuntimeException("Failed to spill to disk", e);
		}
    }

    private String createDataFile(K key) throws IOException {
        File dataFile = new File(Utils.getLocalFilePathForTaskJoin(task, "diskSpillData_" + key.hashCode()+".dat", false, false, false));
        return dataFile.getAbsolutePath();
    }

    private void writeToDisk(String filePath, V value) throws IOException {
        try (FileOutputStream ostream = new FileOutputStream(new File(filePath), true);
        		SnappyOutputStream sos = new SnappyOutputStream(ostream);
        		Output op = new Output(sos);) {
            kryo.writeClassAndObject(op, value);
			op.flush();
		} catch (IOException e) {
			throw new RuntimeException("Failed to write to disk", e);
        }
    }

    private V readFromDisk(String filePath) {
        try (FileInputStream ostream = new FileInputStream(new File(filePath));
        		SnappyInputStream sos = new SnappyInputStream(ostream);
        		Input ip = new Input(sos);) {
            return (V) kryo.readClassAndObject(ip);
        } catch (IOException e) {
            throw new RuntimeException("Failed to read from disk", e);
        }
    }

    @Override
    public V remove(Object key) {
        lock.writeLock().lock();
        try {
            if (inMemoryMap.containsKey(key)) {
                return inMemoryMap.remove(key);
            } else if (keyIndexMap.containsKey(key)) {
                String filePath = keyIndexMap.remove(key);
                new File(filePath).delete(); // Delete the data file
                saveIndex(); // Update the index file
                return null; // The actual value is no longer accessible
            }
            return null;
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void putAll(Map<? extends K, ? extends V> m) {
        lock.writeLock().lock();
        try {
            if(!isspilled) {
            	inMemoryMap.putAll(m);
            } else {
            	m.entrySet().stream().
            	forEach(e -> 
            	put(e.getKey(), e.getValue()));
            }
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public void clear() {
        lock.writeLock().lock();
        try {
            inMemoryMap.clear();
            for (String filePath : keyIndexMap.values()) {
                new File(filePath).delete(); // Delete all data files
            }
            keyIndexMap.clear();
            saveIndex(); // Clear the index file
        } finally {
            lock.writeLock().unlock();
        }
    }

    @Override
    public Set<K> keySet() {
        lock.readLock().lock();
        try {
            Set<K> keys = new HashSet<>(inMemoryMap.keySet());
            keys.addAll(keyIndexMap.keySet());
            return keys;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Collection<V> values() {
        lock.readLock().lock();
        try {
            List<V> values = new ArrayList<>(inMemoryMap.values());
            for (String filePath : keyIndexMap.values()) {
                values.add(readFromDisk(filePath));
            }
            return values;
        } finally {
            lock.readLock().unlock();
        }
    }

    @Override
    public Set<Entry<K, V>> entrySet() {
        lock.readLock().lock();
        try {
            Map<K,V> entries = new HashMap<>(inMemoryMap);
            for (K key : keyIndexMap.keySet()) {
            	if(entries.containsKey(key)) {
            		List entriestoadd = (List) entries.get(key);
            		entriestoadd.add(get(key));
            	} else {
            		entries.put(key, get(key));
            	}
            }
            return entries.entrySet();
        } finally {
            lock.readLock().unlock();
        }
    }

    private void saveIndex() {
        try (ObjectOutputStream oos = new ObjectOutputStream(new FileOutputStream(indexFile))) {
            oos.writeObject(keyIndexMap);
        } catch (IOException e) {
            throw new RuntimeException("Failed to save index file", e);
        }
    }

    private void loadIndex() {
        if (indexFile.exists() && indexFile.length() > 0) {
            try (ObjectInputStream ois = new ObjectInputStream(new FileInputStream(indexFile))) {
                keyIndexMap = (Map<K, String>) ois.readObject();
            } catch (IOException | ClassNotFoundException e) {
                throw new RuntimeException("Failed to load index file", e);
            }
        }
    }

    @Override
    protected void finalize() throws Throwable {
        try {
            saveIndex(); // Ensure the index is saved before the object is garbage collected
        } finally {
            super.finalize();
        }
    }

	public boolean isSpilled() {
		return isspilled;
	}

	public String getAppendwithpath() {
		return appendwithpath;
	}

	public Task getTask() {
		return task;
	}
	
	@Override
	public void close() throws Exception {
		try {
			if (isspilled) {
				if (inMemoryMap.values().size() > 0) {
					inMemoryMap.keySet().stream().forEach(key -> spillToDisk(key, inMemoryMap.get(key)));					
				}
				log.debug("Closing Stream For Task {}", task);								
			}
			this.isclosed = true;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}
	
	public boolean isClosed() {
		return isclosed;
	}
}
