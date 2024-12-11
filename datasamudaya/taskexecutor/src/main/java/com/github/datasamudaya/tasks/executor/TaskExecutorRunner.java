/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor;

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.calcite.rex.RexNode;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.ByteBufferPoolDirectOld;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.CleanupTaskActors;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.EXECUTORTYPE;
import com.github.datasamudaya.common.ExecuteTaskActor;
import com.github.datasamudaya.common.GetTaskActor;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.ShufflePort;
import com.github.datasamudaya.common.SorterPort;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.SummaryWebServlet;
import com.github.datasamudaya.common.Task;
import com.github.datasamudaya.common.TaskExecutorShutdown;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.scheduler.StreamJobScheduler;
import com.github.datasamudaya.stream.utils.SQLUtils;
import com.github.datasamudaya.tasks.executor.web.NodeWebServlet;
import com.github.datasamudaya.tasks.executor.web.ResourcesMetricsServlet;
import com.typesafe.config.Config;

import akka.actor.Address;
import akka.actor.typed.ActorSystem;
import akka.actor.typed.scaladsl.Behaviors;
import akka.cluster.sharding.typed.javadsl.ClusterSharding;
import akka.cluster.sharding.typed.javadsl.EntityTypeKey;
import akka.cluster.typed.Cluster;
import akka.cluster.typed.JoinSeedNodes;
import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * Launches the task executor.
 * 
 * @author arun
 *
 */
public class TaskExecutorRunner implements TaskExecutorRunnerMBean {

	static Logger log = LoggerFactory.getLogger(TaskExecutorRunner.class);
	Map<String, Object> apptaskexecutormap = new ConcurrentHashMap<>();
	Map<String, Object> jobstageexecutormap = new ConcurrentHashMap<>();
	Map<String, Object> actornameactorrefmap = new ConcurrentHashMap<>();
	ConcurrentMap<String, OutputStream> resultstream = new ConcurrentHashMap<>();
	Map<String, Map<String, Object>> jobidstageidexecutormap = new ConcurrentHashMap<>();
	Map<String, Boolean> jobidstageidtaskidcompletedmap = new ConcurrentHashMap<>();
	Map<String, Map<RexNode, AtomicBoolean>> blockspartitionskipmap = new ConcurrentHashMap<>();
	Map<String, JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
	Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
	Map<String, Map<String, EntityTypeKey>> jobidentitytypekeymap = new ConcurrentHashMap<>();
	static ExecutorService estask;
	static ExecutorService escompute;
	static CountDownLatch shutdown = new CountDownLatch(1);
	static ConcurrentMap<BlocksLocation, String> blorcmap = new ConcurrentHashMap<>();
	static Tuple2<ServerSocket, ExecutorService> shuffleFileServer;
	static Tuple2<ServerSocket, ExecutorService> sortServer;
	
	public static void main(String[] args) throws Exception {
		try (var zo = new ZookeeperOperations()) {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			if (args == null || args.length != 4) {
				log.debug("Args" + args);
				if (args != null) {
					log.debug("Args Not of Length 2!=" + args.length);
					for (var arg : args) {
						log.debug(arg);
					}
				}
				System.exit(1);
			}
			if (args.length == 4) {
				log.debug("Args = ");
				for (var arg : args) {
					log.debug(arg);
				}
			}
			String jobid = args[2];
			String executortype = args[3];	
			if (args[0].equals(DataSamudayaConstants.TEPROPLOADDISTROCONFIG)) {
				String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
				PropertyConfigurator.configure(
						datasamudayahome + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER
								+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES);
				Utils.initializeProperties(
						DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
								+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
						DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
			} else if(args[0].equals(DataSamudayaConstants.TEPROPLOADCLASSPATHCONFIG)) {
				Utils.initializePropertiesClasspath(DataSamudayaConstants.FORWARD_SLASH,
						DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
			}
			StaticComponentContainer.Modules.exportAllToAll();
			zo.connect();
			ByteBufferPoolDirectOld.init(Long.parseLong(args[1]));
			ByteBufferPoolDirect.init(Long.parseLong(args[1]));
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
							DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.CACHEBLOCKS + Utils.getCacheID());
			int numberofprocessors = Runtime.getRuntime().availableProcessors();
			ThreadFactory virtualThreadFactory = Thread.ofVirtual().factory();
			estask = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE, 
							DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), virtualThreadFactory);
			escompute = Executors.newFixedThreadPool(Integer.parseInt(DataSamudayaProperties.get()
					.getProperty(DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE, 
							DataSamudayaConstants.VIRTUALTHREADSPOOLSIZE_DEFAULT)), virtualThreadFactory);
			shuffleFileServer = Utils.startShuffleRecordsServer();
			var ter = new TaskExecutorRunner();
			ter.init(zo, jobid, executortype);
			ter.start(zo, jobid, executortype, args);
			int metricsport = Utils.getRandomPort();
			DefaultExports.initialize(); // Initialize JVM metrics
			PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT,
					CollectorRegistry.defaultRegistry, Clock.SYSTEM);
			meterRegistry.config().commonTags("application", DataSamudayaConstants.DATASAMUDAYA.toLowerCase());
			// Bind JVM metrics to the registry
			new JvmMemoryMetrics().bindTo(meterRegistry);
			new JvmThreadMetrics().bindTo(meterRegistry);
			HTTPServer server = new HTTPServer(new InetSocketAddress(metricsport),
					meterRegistry.getPrometheusRegistry());
			// Start an HTTP server to expose metrics
			log.debug("TaskExecuterRunner evoked at metrics port.....{}", metricsport);
			log.debug("TaskExecuterRunner evoked at port..... {}"
			, System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
			log.debug("Reckoning stoppage holder...");
			shutdown.await();
			log.debug("Ceasing the connections...");
			if(nonNull(serverRegistry)) {
				serverRegistry.unbind(DataSamudayaConstants.BINDTESTUB + DataSamudayaConstants.HYPHEN + jobid);
				UnicastRemoteObject.unexportObject(serverRegistry, true);
			}
			server.close();			
			ByteBufferPoolDirect.destroyPool();
			if(nonNull(shuffleFileServer) && nonNull(shuffleFileServer.v1)) {
				shuffleFileServer.v1.close();
			}
			if(nonNull(shuffleFileServer) && nonNull(shuffleFileServer.v2)) {
				shuffleFileServer.v2.shutdown();
			}
			if(nonNull(sortServer) && nonNull(sortServer.v1)) {
				sortServer.v1.close();
			}
			if(nonNull(sortServer) && nonNull(sortServer.v2)) {
				sortServer.v2.shutdown();
			}
			ter.destroy();
			log.debug("Freed the assets for task executor {}...", System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
			System.exit(0);
		} catch (Throwable e) {
			log.error("Error in starting Task Executor: ", e);
		}
		log.debug("Exiting Task Executor {} ...", System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
		return;
	}

	/**
	 * Initializes the zo.
	 */
	@Override
	public void init(ZookeeperOperations zo, String jobid, String executortype) throws Exception {

		var host = NetworkUtil
				.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		var port = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT);

		var hp = host + DataSamudayaConstants.UNDERSCORE + port;

		if(executortype.equalsIgnoreCase(EXECUTORTYPE.EXECUTOR.name())) {
			zo.createTaskExecutorNode(jobid, hp, DataSamudayaConstants.EMPTY.getBytes(), event -> {
				log.debug("TaskExecutor {} initialized and started", hp);
			});
		} else if(executortype.equalsIgnoreCase(EXECUTORTYPE.DRIVER.name())) {
			zo.createDriverNode(jobid, hp, DataSamudayaConstants.EMPTY.getBytes(), event -> {
				log.debug("Driver {} initialized and started", hp);
			});
		}

	}

	ClassLoader cl;
	static Registry serverRegistry;

	/**
	 * Starts and executes the tasks from scheduler via rpc registry.
	 */
	@SuppressWarnings({})
	@Override
	public void start(ZookeeperOperations zo, String jobid, String executortype,String[] args) throws Exception {
		var port = Integer.parseInt(System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
		log.debug("TaskExecutor Port: {}", port);
		var su = new ServerUtils();
		log.debug("Initializing Server at: {}", port);
		if(executortype.equalsIgnoreCase(EXECUTORTYPE.EXECUTOR.name())) {
			log.debug("Executor WebUI initialized at: {}", port + DataSamudayaConstants.PORT_OFFSET);
			su.init(port + DataSamudayaConstants.PORT_OFFSET,
				new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new WebResourcesServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
						+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new ResourcesMetricsServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DATA + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
		} else {
			log.debug("Driver WebUI initialized at: {}", port + DataSamudayaConstants.PORT_OFFSET);
			su.init(port + DataSamudayaConstants.PORT_OFFSET,
					new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new WebResourcesServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
							+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
					new ResourcesMetricsServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DATA + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.ASTERIX, new SummaryWebServlet(),
							DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.SUMMARY_DRIVER 
							+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
					new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
		}
		log.debug("Jetty Server initialized at: {}", port);
		su.start();
		log.debug("Jetty Server started and listening: {}", port);
		var configuration = new Configuration();

		var inmemorycache = DataSamudayaCache.get();
		sortServer = new RemoteIteratorServer(inmemorycache, apptaskexecutormap).start();
		cl = TaskExecutorRunner.class.getClassLoader();
		ActorSystem system = null;
		Cluster cluster;
		final String actorsystemurl;
		ClusterSharding sharding = null;
		if(executortype.equalsIgnoreCase(EXECUTORTYPE.EXECUTOR.name())) {
			while(true) {
				try {
					Config config = Utils.getAkkaSystemConfig(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST
					, DataSamudayaConstants.AKKA_HOST_DEFAULT)
					, Utils.getRandomPort(),
					args[0]);
					system = ActorSystem.create(Behaviors.empty(), DataSamudayaConstants.ACTORUSERNAME, config);
					break;
				} catch(Exception ex) {
					log.error("Unable To Create Akka Actors System...",ex);
					log.debug("Trying to create Akka actor system again...");
				}
			}
			log.debug("Initializing Cluster ...");
			cluster = Cluster.get(system);
			Address address = cluster.selfMember().address();
			log.debug("Initialized Cluster ...");
			List<String> seedNodes = zo.acquireLockAndAddSeedNode(jobid, address.getHost().get() + DataSamudayaConstants.UNDERSCORE 
					+ address.getPort().get());
			log.debug("Seed Nodes {} ...",seedNodes);
			String akkahostport = seedNodes.get(0);
			var addressarray = akkahostport.split(DataSamudayaConstants.UNDERSCORE);
			log.debug("Seed Nodes {}", akkahostport);
			var seednodeaddress = new Address(DataSamudayaConstants.AKKA_URL_SCHEME, DataSamudayaConstants.ACTORUSERNAME, addressarray[0], Integer.parseInt(addressarray[1]));
			log.debug("Seed Nodes Address {}", seednodeaddress);
	        cluster.manager().tell(new JoinSeedNodes(Arrays.asList(seednodeaddress)));
	        log.debug("Joining Seed Nodes Address {}", seednodeaddress);
	        sharding = ClusterSharding.get(system);
			actorsystemurl = DataSamudayaConstants.AKKA_URL_SCHEME + "://" + DataSamudayaConstants.ACTORUSERNAME + "@"
					+ cluster.selfMember().address().getHost().get() + ":" + cluster.selfMember().address().getPort().get() + "/user";
			log.debug("Initializing Sharding ...");
			log.debug("Actor System Url {}", actorsystemurl);
		} else {
			actorsystemurl = "";
			cluster = null;
		}
		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);
		final ActorSystem actsystem = system;
		final ClusterSharding clussharding = sharding;
		dataCruncher = new StreamDataCruncher() {
			public Object postObject(Object deserobj) throws RemoteException {
				Task task = new Task();
				try {
					if (deserobj instanceof byte[] bytes) {
						deserobj = Utils.convertBytesToObjectCompressed(bytes, cl);
					}
					if (deserobj instanceof TaskExecutorShutdown) {
						shutdown.countDown();
					} else if (deserobj instanceof LoadJar loadjar) {
						log.debug("Unpacking jars: " + loadjar.getMrjar());
						cl = DataSamudayaMapReducePhaseClassLoader.newInstance(loadjar.getMrjar(), cl);
						return DataSamudayaConstants.JARLOADED;
					} else if (deserobj instanceof GetTaskActor gettaskactor) {
						if(isNull(jobidentitytypekeymap.get(gettaskactor.getTask().getJobid()))) {
							jobidentitytypekeymap.put(gettaskactor.getTask().getJobid(), new ConcurrentHashMap<String, EntityTypeKey>());
						}
						return SQLUtils.getAkkaActor(actsystem, gettaskactor,
								jobidstageidjobstagemap, hdfs,
								inmemorycache, jobidstageidtaskidcompletedmap,
								actorsystemurl, clussharding, jobid, jobidentitytypekeymap.get(gettaskactor.getTask().getJobid()),
								blockspartitionskipmap);
					} else if (deserobj instanceof ExecuteTaskActor executetaskactor) {
						if(isNull(jobidentitytypekeymap.get(executetaskactor.getTask().getJobid()))) {
							jobidentitytypekeymap.put(executetaskactor.getTask().getJobid(),new ConcurrentHashMap<String, EntityTypeKey>());
						}
						Future<Task> escomputfuture=escompute.submit(()->{
							return SQLUtils.getAkkaActor(actsystem, executetaskactor,
								jobidstageidjobstagemap, hdfs,
								inmemorycache, jobidstageidtaskidcompletedmap,
								actorsystemurl, clussharding, jobid, jobidentitytypekeymap.get(executetaskactor.getTask().getJobid()),
								blockspartitionskipmap);
						});
						return escomputfuture.get();
					} else if (deserobj instanceof CleanupTaskActors cleanupactors) {
						if(jobidentitytypekeymap.containsKey(cleanupactors.getJobid())) {
							Utils.cleanupTaskActorFromSystem(actsystem, jobidentitytypekeymap.remove(cleanupactors.getJobid()), cleanupactors.getJobid());
						}
						return true;
					} else if (deserobj instanceof Job job) {
						job.getPipelineconfig().setClsloader(cl);
						StreamJobScheduler js = new StreamJobScheduler();
						return js.schedule(job);
					} else if (deserobj instanceof ShufflePort) {						
						return shuffleFileServer.v1.getLocalPort();
					} else if (deserobj instanceof SorterPort) {						
						return sortServer.v1().getLocalPort();
					} else if (!Objects.isNull(deserobj)) {
						log.debug("Deserialized object:{} ", deserobj.getClass().getName());
						TaskExecutor taskexecutor = new TaskExecutor(cl, port, escompute, configuration,
								apptaskexecutormap, jobstageexecutormap, resultstream, inmemorycache, deserobj,
								jobidstageidexecutormap, task, jobidstageidjobstagemap, zo, blorcmap,
								jobidstageidtaskidcompletedmap);
						return estask.submit(taskexecutor).get();
					}
				} catch (Throwable ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
					if (ex instanceof Exception e) {
						Utils.getStackTrace(e, task);
					}
				}
				return task;
			}
		};
		log.debug("Getting RPC Registry for port: {}", port);
		serverRegistry = Utils.getRPCRegistry(port, dataCruncher, jobid);
		log.debug("RPC Registry for port: {} Obtained", port);
	}

	static StreamDataCruncher stub;
	static StreamDataCruncher dataCruncher;

	/**
	 * Destroy the thread pool.
	 */
	@Override
	public void destroy() throws Exception {
		if (estask != null) {
			estask.shutdown();
		}
		if (escompute != null) {
			escompute.shutdown();
		}
	}

}
