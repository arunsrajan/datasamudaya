package com.github.datasamudaya.common.utils;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.collections.CollectionUtils;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.cache.ChildData;
import org.apache.curator.framework.recipes.cache.NodeCacheListener;
import org.apache.curator.framework.recipes.cache.CuratorCache;
import org.apache.curator.framework.recipes.cache.CuratorCacheListener;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheEvent.Type;
import org.apache.curator.framework.recipes.cache.PathChildrenCacheListener;
import org.apache.curator.framework.recipes.leader.LeaderLatch;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.framework.recipes.queue.SimpleDistributedQueue;
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
import com.github.datasamudaya.common.DataSamudayaAkkaNodesTaskExecutor;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaNodesResources;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.ZookeeperTasksData;
import com.github.datasamudaya.common.exceptions.ZookeeperException;

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
	 * Creates Akka Actors Node with Task Executor Node Data
	 * @param jobid
	 * @param taskexecutor
	 * @param data
	 * @throws ZookeeperException
	 */
	public void createAkkaActorNodeTaskExecutorNode(String jobid, String akkaactorsnode, byte[] data) throws ZookeeperException {
		try {
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKAACTORSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ akkaactorsnode) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
						.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKAACTORSZK
								+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
								+ akkaactorsnode, data);
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * The method watches the changes in the akka actors node
	 * @param jobid
	 * @throws ZookeeperException
	 */
	public void watchAkkaActorNodeTaskExecutorNode(String jobid) throws ZookeeperException {
		try {
			CuratorCache cache = CuratorCache.build(curator,
					DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKAACTORSZK
					+ DataSamudayaConstants.FORWARD_SLASH + jobid);
			cache.start();
			cache.listenable().addListener((type, oldData, data)-> {
				try {
					switch (type) {
						case NODE_CREATED:
							byte[] changeddata = data.getData();
							String resourcetoadd = new String(changeddata);
							if(!resourcetoadd.equals(DataSamudayaConstants.EMPTY)){								
								String[] nodeadded = data.getPath().split("/");
								if (isNull(DataSamudayaAkkaNodesTaskExecutor.get())) {
									DataSamudayaAkkaNodesTaskExecutor.put(new ConcurrentHashMap<>());
								}
								String currentnode = nodeadded[nodeadded.length - 1];								
								DataSamudayaAkkaNodesTaskExecutor.get().put(currentnode, resourcetoadd);								
							}
							log.debug("Akka node added: {}", data.getPath());
							break;
						case NODE_DELETED:
							String nodetobedeleted = nonNull(oldData)?oldData.getPath():DataSamudayaConstants.EMPTY;
							if(!nodetobedeleted.equals(DataSamudayaConstants.EMPTY)){
								String[] nodetoberemovedarr = nodetobedeleted.split("/");
								if (isNull(DataSamudayaAkkaNodesTaskExecutor.get())) {
									DataSamudayaAkkaNodesTaskExecutor.put(new ConcurrentHashMap<>());
								}
								String nodetoremove = nodetoberemovedarr[nodetoberemovedarr.length - 1];
								DataSamudayaAkkaNodesTaskExecutor.get().remove(nodetoremove);								
								log.debug("Akka node removed: {}",oldData.getPath());
							}							
							break;
						default:
							break;
					}
				} catch (Exception ex) {
					log.error(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
				}
				});
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
		InterProcessSemaphoreMutex semaphore = null;
		try {
			semaphore = new InterProcessSemaphoreMutex(curator, DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK);
			semaphore.acquire();
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid) == null) {
				curator.create().creatingParentsIfNeeded().withMode(CreateMode.PERSISTENT)
						.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
						.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
								+ DataSamudayaConstants.FORWARD_SLASH + jobid, DataSamudayaConstants.EMPTY.getBytes());
			}
			semaphore.release();
			semaphore = new InterProcessSemaphoreMutex(curator, DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.AKKASEEDNODESLOCK
					+ DataSamudayaConstants.FORWARD_SLASH + jobid);
			semaphore.acquire();
			List<String> seednodes = getAkkaSeedNodesByJobId(jobid);
			if (CollectionUtils.isNotEmpty(seednodes)) {
				return seednodes;
			}
			createAkkaSeedNode(jobid, seednode, DataSamudayaConstants.EMPTY.getBytes());
			return Arrays.asList(seednode);
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		} finally {
			if (nonNull(semaphore)) {
				semaphore.release();
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
	public void createTasksForJobNode(Task task, boolean isresultavailable, Watcher watcher) throws ZookeeperException {
		try {
			var zookeepertasksdata = new ZookeeperTasksData();
			zookeepertasksdata.setPrimaryhostport(task.getHostport());
			zookeepertasksdata.setIsresultavailable(isresultavailable);
			byte[] taskbytes = objectMapper.writeValueAsBytes(zookeepertasksdata);
			curator.create().creatingParentsIfNeeded().withMode(CreateMode.EPHEMERAL)
					.withACL(ZooDefs.Ids.OPEN_ACL_UNSAFE)
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
							+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.FORWARD_SLASH
							+ task.getTaskid(), taskbytes);
			curator.getChildren().usingWatcher(watcher).forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + task.getJobid());
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * Update Tasks Data for given Job.
	 * @param jobid
	 * @param task
	 * @param isresultavailable
	 * @throws ZookeeperException
	 */
	public void updateTasksForJobNode(Task task, boolean isresultavailable) throws ZookeeperException {
		try {
			if (curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
							+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.FORWARD_SLASH
							+ task.getTaskid()) == null) {
				createTasksForJobNode(task,isresultavailable, event -> {
					var taskid = task.taskid;
					log.debug("Task {} created in zookeeper", taskid);
				});
			} else {
				byte[] zookeepertasksdatajson = getData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
								+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.FORWARD_SLASH
								+ task.getTaskid());
				var zookeepertasksdata = objectMapper.readValue(zookeepertasksdatajson, ZookeeperTasksData.class); 
				zookeepertasksdata.getResultshardhostports().add(task.getHostport());			
				zookeepertasksdata.setIsresultavailable(isresultavailable);
				setData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
						+ DataSamudayaConstants.FORWARD_SLASH + task.getJobid() + DataSamudayaConstants.FORWARD_SLASH
						+ task.getTaskid(), objectMapper.writeValueAsBytes(zookeepertasksdata));
			}
		} catch (Exception ex) {
			throw new ZookeeperException(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
		}
	}
	
	/**
	 * The function returs tasks data from zookeeper
	 * @param jobid
	 * @param taskid
	 * @return taskdata
	 * @throws ZookeeperException
	 */
	public ZookeeperTasksData getZookeeperTasksDataForJobNode(String jobid, String taskid) throws ZookeeperException {
		try {
			byte[] zookeepertasksdatajson = getData(DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.TASKSZK
							+ DataSamudayaConstants.FORWARD_SLASH + jobid + DataSamudayaConstants.FORWARD_SLASH
							+ taskid);
			var zookeepertasksdata = objectMapper.readValue(zookeepertasksdatajson, ZookeeperTasksData.class); 
			return zookeepertasksdata;
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
			CuratorCache cache = CuratorCache.build(curator,
					DataSamudayaConstants.ROOTZNODEZK + DataSamudayaConstants.NODESZK);
			cache.start();
			cache.listenable().addListener((type, oldData, data)-> {
				try {
					switch (type) {
						case NODE_CREATED:
							byte[] changeddata = data.getData();
							String resourcetoadd = new String(changeddata);
							if(!resourcetoadd.equals(DataSamudayaConstants.EMPTY)){								
								String[] nodeadded = data.getPath().split("/");
								if (isNull(DataSamudayaNodesResources.get())) {
									DataSamudayaNodesResources.put(new ConcurrentHashMap<>());
								}
								if (isNull(DataSamudayaNodesResources.getAllocatedResources())) {
									DataSamudayaNodesResources.putAllocatedResources(new ConcurrentHashMap<>());
								}
								String currentnode = nodeadded[nodeadded.length - 1];
								Resources resources = objectMapper.readValue(changeddata, Resources.class);
								DataSamudayaNodesResources.get().put(currentnode, resources);
								if (isNull(DataSamudayaNodesResources.getAllocatedResources().get(currentnode))) {
									DataSamudayaNodesResources.getAllocatedResources().put(currentnode,
											new ConcurrentHashMap<>());
								}
								Utils.allocateResourcesByUser(resources,
										DataSamudayaNodesResources.getAllocatedResources().get(currentnode));
							}
							log.debug("Master node added: {}", data.getPath());
							break;
						case NODE_DELETED:
							String nodetobedeleted = nonNull(oldData)?oldData.getPath():DataSamudayaConstants.EMPTY;
							if(!nodetobedeleted.equals(DataSamudayaConstants.EMPTY)){
								String[] nodetoberemovedarr = nodetobedeleted.split("/");
								if (isNull(DataSamudayaNodesResources.get())) {
									DataSamudayaNodesResources.put(new ConcurrentHashMap<>());
								}
								String nodetoremove = nodetoberemovedarr[nodetoberemovedarr.length - 1];
								DataSamudayaNodesResources.get().remove(nodetoremove);
								DataSamudayaNodesResources.getAllocatedResources().remove(nodetoremove);
								log.debug("Master node removed: {}",oldData.getPath());
							}							
							break;
						default:
							break;
					}
				} catch (Exception ex) {
					log.error(ZookeeperException.ZKEXCEPTION_MESSAGE, ex);
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
			if(curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK
							+ DataSamudayaConstants.TASKEXECUTORSZK + DataSamudayaConstants.FORWARD_SLASH + jobid) != null) {
				curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKEXECUTORSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			}
			if(curator.checkExists()
					.forPath(DataSamudayaConstants.ROOTZNODEZK
							+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + jobid) != null) {
				curator.delete().guaranteed().deletingChildrenIfNeeded().forPath(DataSamudayaConstants.ROOTZNODEZK
					+ DataSamudayaConstants.TASKSZK + DataSamudayaConstants.FORWARD_SLASH + jobid);
			}
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
			if (curator.checkExists()
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
