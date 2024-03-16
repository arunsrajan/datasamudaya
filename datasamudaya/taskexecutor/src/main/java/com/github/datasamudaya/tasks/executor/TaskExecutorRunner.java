/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor;

import static java.util.Objects.nonNull;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.rmi.RemoteException;
import java.rmi.registry.Registry;
import java.util.Arrays;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jooq.lambda.tuple.Tuple2;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.BlocksLocation;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCache;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.ExecuteTaskActor;
import com.github.datasamudaya.common.GetTaskActor;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.JobStage;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.ShufflePort;
import com.github.datasamudaya.common.StreamDataCruncher;
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

import akka.actor.ActorSystem;
import akka.cluster.Cluster;
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
	Map<String, JobStage> jobidstageidjobstagemap = new ConcurrentHashMap<>();
	Queue<Object> taskqueue = new LinkedBlockingQueue<Object>();
	static ExecutorService estask;
	static ExecutorService escompute;
	static CountDownLatch shutdown = new CountDownLatch(1);
	static ConcurrentMap<BlocksLocation, String> blorcmap = new ConcurrentHashMap<>();
	static Tuple2<ServerSocket, ExecutorService> shuffleFileServer;
	
	public static void main(String[] args) throws Exception {
		try (var zo = new ZookeeperOperations()) {
			URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
			if (args == null || args.length != 3) {
				log.debug("Args" + args);
				if (args != null) {
					log.debug("Args Not of Length 2!=" + args.length);
					for (var arg : args) {
						log.debug(arg);
					}
				}
				System.exit(1);
			}
			if (args.length == 3) {
				log.debug("Args = ");
				for (var arg : args) {
					log.debug(arg);
				}
			}
			String jobid = args[2];			
			shuffleFileServer = Utils.startShuffleRecordsServer();
			String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
			PropertyConfigurator.configure(
					datasamudayahome + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER
							+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES);
			if (args[0].equals(DataSamudayaConstants.TEPROPLOADDISTROCONFIG)) {
				Utils.initializeProperties(
						DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
								+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH,
						DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
			}
			StaticComponentContainer.Modules.exportAllToAll();
			zo.connect();
			ByteBufferPoolDirect.init(Long.parseLong(args[1]));
			CacheUtils.initCache(DataSamudayaConstants.BLOCKCACHE,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
							DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.CACHEBLOCKS + Utils.getCacheID());
			int numberofprocessors = Runtime.getRuntime().availableProcessors();
			estask = new ThreadPoolExecutor(numberofprocessors, numberofprocessors, 60, TimeUnit.SECONDS,
					new LinkedBlockingQueue());
			escompute = new ThreadPoolExecutor(numberofprocessors, numberofprocessors, 60, TimeUnit.SECONDS,
					new LinkedBlockingQueue());
			var ter = new TaskExecutorRunner();
			ter.init(zo, jobid);
			ter.start(zo, jobid);
			int metricsport = Integer
					.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT)) + 200;
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
			log.info("TaskExecuterRunner evoked at metrics port.....{}", metricsport);
			log.info("TaskExecuterRunner evoked at port..... {}"
			, System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
			log.info("Reckoning stoppage holder...");
			shutdown.await();
			log.info("Ceasing the connections...");
			server.close();
			ter.destroy();
			ByteBufferPoolDirect.destroy();
			if(nonNull(shuffleFileServer) && nonNull(shuffleFileServer.v1)) {
				shuffleFileServer.v1.close();
			}
			if(nonNull(shuffleFileServer) && nonNull(shuffleFileServer.v2)) {
				shuffleFileServer.v2.shutdown();
			}
			log.info("Freed the assets...");
			System.exit(0);
		} catch (Throwable e) {
			log.error("Error in starting Task Executor: ", e);
		}
	}

	/**
	 * Initializes the zo.
	 */
	@Override
	public void init(ZookeeperOperations zo, String jobid) throws Exception {

		var host = NetworkUtil
				.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		var port = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT);

		var hp = host + DataSamudayaConstants.UNDERSCORE + port;

		zo.createTaskExecutorNode(jobid, hp, DataSamudayaConstants.EMPTY.getBytes(), event -> {
			log.info("TaskExecutor {} initialized and started", hp);
		});

	}

	ClassLoader cl;
	static Registry server;

	/**
	 * Starts and executes the tasks from scheduler via rpc registry.
	 */
	@SuppressWarnings({})
	@Override
	public void start(ZookeeperOperations zo, String jobid) throws Exception {
		var port = Integer.parseInt(System.getProperty(DataSamudayaConstants.TASKEXECUTOR_PORT));
		log.info("TaskExecutor Port: {}", port);
		var su = new ServerUtils();
		log.info("Initializing Server at: {}", port);
		su.init(port + DataSamudayaConstants.PORT_OFFSET,
				new NodeWebServlet(new ConcurrentHashMap<String, Map<String, Process>>()),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new WebResourcesServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
						+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new ResourcesMetricsServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DATA + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
		log.info("Jetty Server initialized at: {}", port);
		su.start();
		log.info("Jetty Server started and listening: {}", port);
		var configuration = new Configuration();

		var inmemorycache = DataSamudayaCache.get();
		cl = TaskExecutorRunner.class.getClassLoader();
		ActorSystem system = null;
		while(true) {
			try {
				Config config = Utils.getAkkaSystemConfig(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.AKKA_HOST
				, DataSamudayaConstants.AKKA_HOST_DEFAULT)
				, Utils.getRandomPort(),
						Runtime.getRuntime().availableProcessors());
				system = ActorSystem.create(DataSamudayaConstants.ACTORUSERNAME, config);
				break;
			} catch(Exception ex) {
				log.error("Unable To Create Akka Actors System...",ex);
				log.info("Trying to create Akka actor system again...");
			}
		}
		Cluster cluster = Cluster.get(system);
		cluster.joinSeedNodes(Arrays.asList(cluster.selfAddress()));

		final String actorsystemurl = DataSamudayaConstants.AKKA_URL_SCHEME + "://" + DataSamudayaConstants.ACTORUSERNAME + "@"
				+ system.provider().getDefaultAddress().getHost().get() + ":" + system.provider().getDefaultAddress().getPort().get() + "/user";

		log.info("Actor System Url {}", actorsystemurl);

		var hdfsfilepath = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
				DataSamudayaConstants.HDFSNAMENODEURL_DEFAULT);
		var hdfs = FileSystem.newInstance(new URI(hdfsfilepath), configuration);
		var host = NetworkUtil
				.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
		final ActorSystem actsystem = system;
		dataCruncher = new StreamDataCruncher() {
			public Object postObject(Object deserobj) throws RemoteException {
				Task task = new Task();
				try {
					if (deserobj instanceof byte[] bytes) {
						try (ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
								var fstin = new Input(bais);) {
							Kryo kryo = Utils.getKryoInstance();
							kryo.setClassLoader(cl);
							deserobj = kryo.readClassAndObject(fstin);
						}
					}
					if (deserobj instanceof TaskExecutorShutdown) {
						shutdown.countDown();
					} else if (deserobj instanceof LoadJar loadjar) {
						log.info("Unpacking jars: " + loadjar.getMrjar());
						cl = DataSamudayaMapReducePhaseClassLoader.newInstance(loadjar.getMrjar(), cl);
						return DataSamudayaConstants.JARLOADED;
					} else if (deserobj instanceof GetTaskActor gettaskactor) {
						return SQLUtils.getAkkaActor(actsystem, gettaskactor,
								jobidstageidjobstagemap, hdfs,
								inmemorycache, jobidstageidtaskidcompletedmap,
								actorsystemurl, cluster, jobid);
					} else if (deserobj instanceof ExecuteTaskActor executetaskactor) {
						return SQLUtils.getAkkaActor(actsystem, executetaskactor,
								jobidstageidjobstagemap, hdfs,
								inmemorycache, jobidstageidtaskidcompletedmap,
								actorsystemurl, cluster, jobid);
					} else if (deserobj instanceof Job job) {
						job.getPipelineconfig().setClsloader(cl);
						StreamJobScheduler js = new StreamJobScheduler();
						return js.schedule(job);
					} else if (deserobj instanceof ShufflePort) {						
						return shuffleFileServer.v1.getLocalPort();
					} else if (!Objects.isNull(deserobj)) {
						log.info("Deserialized object:{} ", deserobj);
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
		log.info("Getting RPC Registry for port: {}", port);
		server = Utils.getRPCRegistry(port, dataCruncher, jobid);
		log.info("RPC Registry for port: {} Obtained", port);
	}

	static StreamDataCruncher stub;
	static StreamDataCruncher dataCruncher;

	/**
	 * Destroy the thread pool.
	 */
	@Override
	public void destroy() throws Exception {
		if (estask != null) {
			estask.shutdownNow();
			estask.awaitTermination(1, TimeUnit.SECONDS);
		}
		if (escompute != null) {
			escompute.shutdownNow();
			escompute.awaitTermination(1, TimeUnit.SECONDS);
		}
	}

}
