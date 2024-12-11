package com.github.datasamudaya.common.utils;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCache;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCache;
import org.apache.curator.framework.recipes.cache.PathChildrenCache.StartMode;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.annotation.JsonInclude.Include;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.exceptions.ZookeeperException;

import static java.util.Objects.*;

/**
 * @author arun
 * Zookeeper Operations required for creating tasks, nodes, SCHEDULERSZK and TASKEXECUTORSZK
 */
public class ZookeeperOperations implements AutoCloseable {
	private CuratorFramework curator;
	private ObjectMapper objectMapper;
	private static final Logger log = LoggerFactory.getLogger(ZookeeperOperations.class);

	public void connect() throws ZookeeperException {
		try {
			curator = CuratorFrameworkFactory.newClient(
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_HOSTPORT,
							DataSamudayaConstants.ZK_DEFAULT),
					new RetryForever(Integer.parseInt(
							DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_RETRYDELAY,
									DataSamudayaConstants.ZOOKEEPER_RETRYDELAY_DEFAULT))));
			curator.start();
			curator.blockUntilConnected();
			objectMapper = new ObjectMapper();
			objectMapper.setSerializationInclusion(Include.NON_NULL);
			objectMapper.disable(SerializationFeature.FAIL_ON_EMPTY_BEANS);
		} catch (InterruptedException e) {
			log.warn(DataSamudayaConstants.INTERRUPTED, e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  Creates root node by path and set the data for the root node
	  @param path
		 @param data
		 @throws Exception
		*/
	public void createRootNode(String path, byte[] data) throws ZookeeperException {
		try {
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT).withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(path, data);
			Watcher watcher = (WatchedEvent event) -> log.debug("Root node changed: {}", event);
			curator.getData().usingWatcher(watcher).forPath(path);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method creates nodes in zookeeper with the node path, setting the data and watcher.
	  @param node
		 @param data
		 @param watcher
		 @throws Exception
		*/
	public void createNodesNode(String node, Resources data, Watcher watcher) throws ZookeeperException {
		try {
			if (curator.checkExists().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.NODESZK + DataSamudayaConstants.FORWARD_SLASH + node) != null) {
				deleteNode(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.NODESZK + DataSamudayaConstants.FORWARD_SLASH + node);
			}
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.NODESZK + DataSamudayaConstants.FORWARD_SLASH + node,
					objectMapper.writeValueAsBytes(data));
			curator.getChildren().usingWatcher(watcher)
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.NODESZK);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method creates schedulers with leader election with setting the data and watcher.
	  @param data
		 @param watcher
		 @throws Exception
		*/
	public void createSchedulersLeaderNode(byte[] data, Watcher watcher) throws ZookeeperException {
		try {
			if (curator.checkExists().forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERZK) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.LEADERZK + DataSamudayaConstants.LEADERSCHEDULERZK, data);
				curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.LEADERZK + DataSamudayaConstants.LEADERSCHEDULERZK);
			}
			if (curator.checkExists().forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERSTREAMZK) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.LEADERZK + DataSamudayaConstants.LEADERSCHEDULERSTREAMZK, data);
				curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.LEADERZK + DataSamudayaConstants.LEADERSCHEDULERSTREAMZK);
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  Creates distributed queue
	  @param path
		 @return Distributed queue object
	 */
	public SimpleDistributedQueue createDistributedQueue(String path) {
		return new SimpleDistributedQueue(curator, path);
	}

	/**
	  This method creates the task executor znode in zookeeper with the jobid and taskexecutor, data and watcher.
	  @param jobid
		 @param taskexecutor
		 @param data
		 @param watcher
		 @throws Exception
		*/
	public void createTaskExecutorNode(String jobid, String taskexecutor, byte[] data, Watcher watcher) throws ZookeeperException {
		try {
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKEXECUTORSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ taskexecutor) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
						.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKEXECUTORSZK
								+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
								+ taskexecutor, data);
				curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.TASKEXECUTORSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * Creates Akka Seed nodes for a given jobid in zookeeper
	 * @param jobid
	 * @param seednode
	 * @param data
	 * @throws ZookeeperException
	 */
	public void createAkkaSeedNode(String jobid, String seednode, byte[] data) throws ZookeeperException {
		try {			
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESZKROOT
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ seednode) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
						.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESZKROOT
								+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
								+ seednode, data);
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * The function acquires lock for a given jobid and returns seed nodes
	 * @param jobid
	 * @return returns seed node
	 * @throws Exception
	 */
	public List<String> acquireLockAndAddSeedNode(String jobid, String seednode) throws Exception {
		InterProcessSemaphoreV2 semaphore = null;
		Lease lease = null;
		try {
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
				.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
				.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
						+ DataSamudayaConstants.FORWARD_SLASH + jobid, DataSamudayaConstants.EMPTY.getBytes());
			}
			semaphore = new InterProcessSemaphoreV2(curator, DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
					+ DataSamudayaConstants.FORWARD_SLASH + jobid, 1);
			lease =  semaphore.acquire();
			List<String> seednodes = getAkkaSeedNodesByJobId(jobid);
			if(CollectionUtils.isNotEmpty(seednodes)) {
				return seednodes;
			}
			createAkkaSeedNode(jobid, seednode, DataSamudayaConstants.EMPTY.getBytes());
			return Arrays.asList(seednode);			
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		} finally {
			if(nonNull(semaphore) && nonNull(lease)) {
				semaphore.returnLease(lease);
			}
		}
	}
	
	/**
	 * The method creates the driver znode in zookeeper with the jobid and driver, data and watcher.
	 * @param jobid
	 * @param driver
	 * @param data
	 * @param watcher
	 * @throws ZookeeperException
	 */
	public void createDriverNode(String jobid, String driver, byte[] data, Watcher watcher) throws ZookeeperException {
		try {
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.DRIVERSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ driver) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
						.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.DRIVERSZK
								+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
								+ driver, data);
				curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
						+ DataSamudayaConstants.DRIVERSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	
	/**
	  This method creates the znodes for tasks given jobid, task and watcher. 
	  @param jobid
		 @param task
		 @param watcher
		 @throws Exception
		*/
	public void createTasksForJobNode(String jobid, Task task, Watcher watcher) throws ZookeeperException {
		try {
			byte[] taskbytes = objectMapper.writeValueAsBytes(task);
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ task.getTaskid(), taskbytes);
			curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	public void watchTaskNode(String jobid, String path) throws ZookeeperException {
		try {
			NodeCache cache = new NodeCache(curator, DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
					+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH + path);
			cache.start();
			cache.getListenable().addListener(new NodeCacheListener() {
				@Override
				public void nodeChanged() throws Exception {
					ChildData data = cache.getCurrentData();
					if (data != null) {
						log.debug("Task executor node changed: {}", new String(data.getData()));
					} else {
						log.debug("Task executor node deleted: {}", path);
					}
				}
			});
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method creates the watcher for the nodes added and updates the global resources.
	  @throws Exception
		*/
	public void watchNodes() throws ZookeeperException {
		try {
			PathChildrenCache cache = new PathChildrenCache(curator,
					DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.NODESZK, true);
			cache.start(StartMode.POST_INITIALIZED_EVENT);
			cache.getListenable().addListener(new PathChildrenCacheListener() {
				@Override
				public void childEvent(CuratorFramework curator, PathChildrenCacheEvent event) throws Exception {
					Type type = event.getType();
					switch (type) {
						case CHILD_ADDED:
							String[] nodeadded = event.getData().getPath().split("/");
							if (isNull(DataSamudayaNodesResources.get())) {
								DataSamudayaNodesResources.put(new ConcurrentHashMap<>());
							}
							if (isNull(DataSamudayaNodesResources.getAllocatedResources())) {
								DataSamudayaNodesResources.putAllocatedResources(new ConcurrentHashMap<>());
							}
							String currentnode = nodeadded[nodeadded.length - 1];
							Resources resources = objectMapper.readValue(event.getData().getData(), Resources.class);
							DataSamudayaNodesResources.get().put(currentnode, resources);
							if (isNull(DataSamudayaNodesResources.getAllocatedResources().get(currentnode))) {
								DataSamudayaNodesResources.getAllocatedResources().put(currentnode,
										new ConcurrentHashMap<>());
							}
							Utils.allocateResourcesByUser(resources,
									DataSamudayaNodesResources.getAllocatedResources().get(currentnode));
							log.debug("Master node added: {}", event.getData().getPath());
							break;
						case CHILD_REMOVED:
							String[] nodetoberemoved = event.getData().getPath().split("/");
							if (isNull(DataSamudayaNodesResources.get())) {
								DataSamudayaNodesResources.put(new ConcurrentHashMap<>());
							}
							String nodetoremove = nodetoberemoved[nodetoberemoved.length - 1];
							DataSamudayaNodesResources.get().remove(nodetoremove);
							DataSamudayaNodesResources.getAllocatedResources().remove(nodetoremove);

							log.debug("Master node removed: {}", event.getData().getPath());
							break;
						default:
							break;
					}
				}
			});
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method creates the leader nodes for the map reduce scheduler
	  @param scheduler
		 @param listener
		 @return leaderlatch object.
	  @throws Exception
		*/
	public LeaderLatch leaderElectionScheduler(String scheduler, LeaderLatchListener listener) throws ZookeeperException {
		try {
			LeaderLatch leaderLatch = new LeaderLatch(curator,
					DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.SCHEDULERSZK, scheduler);
			leaderLatch.addListener(listener);
			leaderLatch.start();
			return leaderLatch;
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method creates the leader nodes for the stream scheduler
	  @param scheduler
		 @param listener
		 @return leaderlatch object.
	  @throws Exception
		*/
	public LeaderLatch leaderElectionSchedulerStream(String scheduler, LeaderLatchListener listener) throws ZookeeperException {
		try {
			LeaderLatch leaderLatch = new LeaderLatch(curator, DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.SCHEDULERSSTREAMZK, scheduler);
			leaderLatch.addListener(listener);
			leaderLatch.start();
			return leaderLatch;
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method returns the data for the given path.
	  @param path
		 @return byte data
	  @throws Exception
		*/
	public byte[] getData(String path) throws ZookeeperException {
		try {
			return curator.getData().forPath(path);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method sets the data for the given znode path.
	  @param path
		 @param data
		 @throws Exception
		*/
	public void setData(String path, byte[] data) throws ZookeeperException {
		try {
			// Set the data for the specified path
			curator.setData().forPath(path, data);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method set the leader for the mr scheduler
	  @param data
		 @throws Exception
		*/
	public void setLeader(byte[] data) throws ZookeeperException {
		try {
			curator.setData().forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERZK, data);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method set the leader for the streaming scheduler
	  @param data
		 @throws Exception
		*/
	public void setLeaderStream(byte[] data) throws ZookeeperException {
		try {
			curator.setData().forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERSTREAMZK, data);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method set the task information for the given task and jobid.
	  @param jobid
		 @param task
		 @throws Exception
		*/
	public void setDataForTask(String jobid, Task task) throws ZookeeperException {
		try {
			byte[] taskbytes = objectMapper.writeValueAsBytes(task);
			curator.setData()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ task.getTaskid(), taskbytes);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	public void deleteNode(String path) throws ZookeeperException {
		try {
			// Delete the node for the specified path
			curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This method deletes the job entries for the given job id.
	  @param jobid
		 @throws Exception
		*/
	public void deleteJob(String jobid) throws ZookeeperException {
		try {
			// Delete the node for the specified path
			curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKEXECUTORSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This function returns all the nodes created in zookeeper.
	  @return list of nodes in string format.
	  @throws Exception
		*/
	public List<String> getNodes() throws ZookeeperException {
		try {
			return curator.getChildren()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.NODESZK);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This function returns list of task executors.
	  @param jobid
		 @return list of te's
	  @throws Exception
		*/
	public List<String> getTaskExectorsByJobId(String jobid) throws ZookeeperException {
		try {
			return curator.getChildren().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKEXECUTORSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * The function returns all the drivers registered for the given job id 
	 * @param jobid
	 * @return list of drivers registered in zookeeper
	 * @throws ZookeeperException
	 */
	public List<String> getDriversByJobId(String jobid) throws ZookeeperException {
		try {
			return curator.getChildren().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.DRIVERSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  This function returns the list of tasks by jobid.
	  @param jobid
		 @return list of tasks
	  @throws Exception
		*/
	public List<Task> getTasksByJobId(String jobid) throws ZookeeperException {
		try {
			List<String> tasks = curator.getChildren().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			var taskdeserialized = new ArrayList<Task>();
			for (String task : tasks) {
				byte[] taskbytes = getData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
						+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH + task);
				taskdeserialized.add(objectMapper.readValue(taskbytes, Task.class));
			}
			return taskdeserialized;
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  Gets the master of the stream scheduler.
	  @return master of stream scheduler
	  @throws Exception
		*/
	public String getStreamSchedulerMaster() throws ZookeeperException {
		try {
			return new String(getData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERSTREAMZK));
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	  Gets the master of the map reduce scheduler.
	  @return master of mr's
	  @throws Exception
		*/
	public String getMRSchedulerMaster() throws ZookeeperException {
		try {
			return new String(getData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.LEADERZK
					+ DataSamudayaConstants.LEADERSCHEDULERZK));
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}

	/**
	 * THe function returns akka seed nodes by jobid
	 * @param jobid
	 * @return seed nodes
	 * @throws ZookeeperException
	 */
	public List<String> getAkkaSeedNodesByJobId(String jobid) throws ZookeeperException {
		try {
			if(curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK
							+ DataSamudayaConstants.AKKASEEDNODESZKROOT + DataSamudayaConstants.FORWARD_SLASH + jobid) != null) {
				return curator.getChildren().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.AKKASEEDNODESZKROOT + DataSamudayaConstants.FORWARD_SLASH + jobid);
			}
			return Arrays.asList();
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	  Close the curator object for closing the zookeeper connection.
	 */
	@Override
	public void close() {
		curator.close();
	}
}
