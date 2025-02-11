package com.github.datasamudaya.tasks.scheduler.executor.standalone;

import java.io.DataInputStream;
import java.io.FileInputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.net.URL;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
import java.util.stream.Collectors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.logging.log4j.core.config.ConfigurationSource;
import org.apache.logging.log4j.core.config.Configurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.ByteBufferPoolDirect;
import com.github.datasamudaya.common.ByteBufferPoolDirectOld;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.SummaryWebServlet;
import com.github.datasamudaya.common.TaskExecutorDestroyServlet;
import com.github.datasamudaya.common.TaskSchedulerWebServlet;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.JShellServer;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.pig.PigQueryServer;
import com.github.datasamudaya.stream.scheduler.StreamPipelineTaskScheduler;
import com.github.datasamudaya.stream.sql.SQLServer;
import com.github.datasamudaya.stream.utils.PipelineGraphWebServlet;
import com.github.datasamudaya.tasks.executor.NodeRunner;
import com.github.datasamudaya.tasks.executor.web.NodeWebServlet;
import com.github.datasamudaya.tasks.executor.web.ResourcesMetricsServlet;
import com.github.datasamudaya.tasks.scheduler.TaskScheduler;
import com.github.datasamudaya.tasks.scheduler.sql.SQLServerMR;

import io.micrometer.core.instrument.Clock;
import io.micrometer.core.instrument.binder.jvm.JvmMemoryMetrics;
import io.micrometer.core.instrument.binder.jvm.JvmThreadMetrics;
import io.micrometer.prometheus.PrometheusConfig;
import io.micrometer.prometheus.PrometheusMeterRegistry;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;

/**
 * This class starts the stream scheduler, mr scheduler and node launcher.
 * @author arun
 *
 */
public class EmbeddedSchedulersNodeLauncher {
	
	static {
		System.setProperty("log4j.configurationFile", 
				System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME) + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J2_PROPERTIES);
	}
	
	static Logger log = LoggerFactory.getLogger(EmbeddedSchedulersNodeLauncher.class);

	public static final String STOPPINGANDCLOSECONNECTION = "Stopping and closes all the connections...";
	private static final Semaphore lock = new Semaphore(1);
	private static final CountDownLatch cdlcl = new CountDownLatch(1);

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);		
		var options = new Options();
		options.addOption(DataSamudayaConstants.CONF, true, DataSamudayaConstants.EMPTY);
		var parser = new DefaultParser();
		var cmd = parser.parse(options, args);

		String config = null;
		if (cmd.hasOption(DataSamudayaConstants.CONF)) {
			config = cmd.getOptionValue(DataSamudayaConstants.CONF);
			Utils.initializeProperties(DataSamudayaConstants.EMPTY, config);
		} else {
			Utils.initializeProperties(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		}
		StaticComponentContainer.Modules.exportAllToAll();
		var cdl = new CountDownLatch(3);
		int metricsport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.METRICS_EXPORTER_PORT,
				DataSamudayaConstants.METRICS_EXPORTER_PORT_DEFAULT));
		DefaultExports.initialize(); // Initialize JVM metrics    	 
		PrometheusMeterRegistry meterRegistry = new PrometheusMeterRegistry(PrometheusConfig.DEFAULT, CollectorRegistry.defaultRegistry, Clock.SYSTEM);
		meterRegistry.config().commonTags("application", DataSamudayaConstants.DATASAMUDAYA.toLowerCase());
		// Bind JVM metrics to the registry
		new JvmMemoryMetrics().bindTo(meterRegistry);
		new JvmThreadMetrics().bindTo(meterRegistry);
		// Start an HTTP server to expose metrics
		try (var zo = new ZookeeperOperations();
				HTTPServer server = new HTTPServer(new InetSocketAddress(metricsport), meterRegistry.getPrometheusRegistry());) {
			zo.connect();
			zo.createSchedulersLeaderNode(DataSamudayaConstants.EMPTY.getBytes(), event -> {
				log.debug("Node Created");
			});
			zo.watchNodes();
			ByteBufferPoolDirectOld.init(1 * DataSamudayaConstants.GB);
			ByteBufferPoolDirect.init(2 * DataSamudayaConstants.GB);
			String cacheid = DataSamudayaConstants.BLOCKCACHE;
			CacheUtils.initCache(cacheid,
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
							DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.CACHEBLOCKS);
			CacheUtils.initBlockMetadataCache(cacheid);
			ExecutorService es = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("Launcher-", 0).factory());
			es.execute(new Runnable() {
				public void run() {
					try {
						startContainerLauncher(zo, cdl);
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				}
			});
			es.execute(new Runnable() {
				public void run() {
					try {
						cdlcl.await();
						startTaskScheduler(zo, cdl);
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				}
			});
			es.execute(new Runnable() {
				public void run() {
					try {
						cdlcl.await();
						startTaskSchedulerStream(zo, cdl);
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				}
			});
			String nodeport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT);
			String streamport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT);
			String streamwebport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_WEB_PORT);
			String mrport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT);
			String mrwebport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_WEB_PORT);
			log.debug(
					"Program evoked in the port Stream[port={},webport={}] MapReduce[port={},webport={}] Node[port={}]",
					streamport, streamwebport, mrport, mrwebport, nodeport);
			cdl.await();
			es.shutdown();
			log.debug("Schedulers Resources Cleaned...");
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
		Runtime.getRuntime().halt(0);
	}

	static Registry server;

	public static void startContainerLauncher(ZookeeperOperations zo, CountDownLatch cdl) {
		try {
			var port = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
			var host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
			var escontainer = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("ContainerLauncher-", 0).factory());
			Resources resource = new Resources();
			resource.setNodeport(host + DataSamudayaConstants.UNDERSCORE + port);
			resource.setTotalmemory(Runtime.getRuntime().totalMemory());
			resource.setFreememory(Utils.getTotalAvailablePhysicalMemory());
			resource.setNumberofprocessors(Utils.getAvailableProcessors());
			resource.setTotaldisksize(Utils.totaldiskspace());
			resource.setUsabledisksize(Utils.usablediskspace());
			resource.setPhysicalmemorysize(Utils.getPhysicalMemory());
			zo.createNodesNode(host + DataSamudayaConstants.UNDERSCORE + port, resource, event -> {
				log.debug("{}", event);
			});
			var hdfs = FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL)),
					new Configuration());
			var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
			var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
			var containeridports = new ConcurrentHashMap<String, List<Integer>>();
			var su = new ServerUtils();
			su.init(port + DataSamudayaConstants.PORT_OFFSET, new NodeWebServlet(containerprocesses),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new WebResourcesServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.ASTERIX,
					new ResourcesMetricsServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DATA + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
					new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
			su.start();
			datacruncher = new StreamDataCruncher() {
				public Object postObject(Object object) {
					try {
						if (object instanceof byte[] bytes) {
							object = Utils.convertBytesToObjectCompressed(bytes, null);
						}
						var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER, containerprocesses, hdfs,
								containeridthreads, containeridports, object, zo);
						Future<Object> containerallocated = escontainer.submit(container);
						Object retobj = containerallocated.get();
						log.debug("Node processor refined the {} with status {} ", object, retobj);
						return retobj;
					} catch (InterruptedException e) {
						log.warn("Interrupted!", e);
						// Restore interrupted state...
						Thread.currentThread().interrupt();
					} catch (Exception e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
					return null;
				}
			};
			server = Utils.getRPCRegistry(port, datacruncher, DataSamudayaConstants.EMPTY);
			log.debug("NodeLauncher started at port....." + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
			log.debug("Adding Shutdown Hook...");
			cdlcl.countDown();
			Utils.addShutdownHook(() -> {
				try {
					containerprocesses
							.keySet().stream().map(containerprocesses::get).flatMap(mapproc -> mapproc.keySet().stream()
							.map(key -> mapproc.get(key)).collect(Collectors.toList()).stream())
							.forEach(proc -> {
								log.debug("Destroying the Container Process: " + proc);
								proc.destroy();
							});
					log.debug(STOPPINGANDCLOSECONNECTION);
					log.debug("Destroying...");
					if (Objects.nonNull(hdfs)) {
						hdfs.close();
					}
					if (Objects.nonNull(server)) {
						UnicastRemoteObject.unexportObject(server, true);
					}
					cdl.countDown();
					log.debug("Container Launcher Resources Cleaned...");
					lock.acquire();
					if (Objects.nonNull(DataSamudayaCacheManager.get())) {
						DataSamudayaCacheManager.get().close();
						DataSamudayaCacheManager.put(null);
					}
					lock.release();
				} catch (Exception e) {
					log.debug("", e);
				}
			});
		} catch (Exception ex) {
			log.error("Unable to start Node Manager due to ", ex);
		}
	}

	static StreamDataCruncher stub;
	static StreamDataCruncher datacruncher;

	public static void startTaskSchedulerStream(ZookeeperOperations zo, CountDownLatch cdl) throws Exception {
		var cdlstream = new CountDownLatch(1);
		var zookeeperid = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOST))
				+ DataSamudayaConstants.UNDERSCORE + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT);
		zo.leaderElectionSchedulerStream(zookeeperid, new LeaderLatchListener(){

			@Override
			public void isLeader() {
				log.debug("Stream Scheduler Node {} elected as leader", zookeeperid);
				try {
					zo.setLeaderStream(zookeeperid.getBytes());
					cdlstream.countDown();
				} catch (Exception e) {
				}
			}

			@Override
			public void notLeader() {
			}

		});
		log.debug("Streaming Scheduler Waiting to elect as a leader...");
		cdlstream.await();
		var esstream = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("EmbeddedTSStream-", 0).factory());
		var es = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("EmbeddedExecutor-", 0).factory());
		var su = new ServerUtils();
		su.init(Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
				new TaskSchedulerWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new PipelineGraphWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.GRAPH, new SummaryWebServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.SUMMARY + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON,
				new TaskExecutorDestroyServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.KILL_EXECUTOR_URL + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.ASTERIX);
		su.start();
		var lbq = new LinkedBlockingQueue<StreamPipelineTaskScheduler>(Integer.valueOf(
				DataSamudayaProperties.get().getProperty(DataSamudayaConstants.DATASAMUDAYAJOBQUEUE_SIZE, DataSamudayaConstants.DATASAMUDAYAJOBQUEUE_SIZE_DEFAULT)));

		var execkind = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.EXEC_KIND, DataSamudayaConstants.EXEC_KIND_DEFAULT);

		var isparallel = execkind.equals(DataSamudayaConstants.EXEC_KIND_PARALLEL);

		// Execute when request arrives.
		esstream.execute(() -> {
			try (var ss = new ServerSocket(
					Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT)));) {
				while (true) {
					try {
						var s = ss.accept();
						var bytesl = new ArrayList<byte[]>();
						var in = new DataInputStream(s.getInputStream());
						while (true) {
							var len = in.readInt();
							byte buffer[] = new byte[len]; // this could be reused !
							while (len > 0) {
								len -= in.read(buffer, buffer.length - len, len);
							}
							// skipped: check for stream close
							Object obj = Utils.getKryo().readClassAndObject(new Input(buffer));
							if (obj instanceof Integer brkintval && brkintval == -1) {
								break;
							}
							bytesl.add((byte[]) obj);
						}
						String[] arguments = null;
						if (bytesl.size() > 2) {
							var totalargs = bytesl.size();
							arguments = new String[totalargs - 1];
							for (var index = 2;index < totalargs;index++) {
								arguments[index - 2] = new String(bytesl.get(index));
							}
						}
						// Execute concurrently through thread pool
						// executors
						var filename = new String(bytesl.get(1));
						log.debug("Queueing the Job Name: {}", filename);
						var spts = new StreamPipelineTaskScheduler(filename, bytesl.get(0), arguments, s);
						if (!isparallel) {
							lbq.put(spts);
						} else {
							es.execute(spts);
						}
					} catch (Exception ex) {
						log.error("Launching Stream Task scheduler error, See cause below \n", ex);
					}
				}
			} catch (Exception ex) {

			}
		});
		if (!isparallel) {
			es.execute(() -> {
				StreamPipelineTaskScheduler spts;
				while (true) {
					while ((lbq.peek()) != null) {
						spts = lbq.poll();
						spts.run();
					}
					try {
						Thread.sleep(1000);
					} catch (InterruptedException e) {
					}
				}
			});
		}
		Utils.addShutdownHook(() -> {
			try {
				log.debug(STOPPINGANDCLOSECONNECTION);
				log.debug("Destroying...");
				if (!Objects.isNull(es)) {
					es.shutdown();
				}
				if (!Objects.isNull(esstream)) {
					esstream.shutdown();
				}
				if (!Objects.isNull(su)) {
					su.stop();
					su.destroy();
				}
				cdl.countDown();
				log.debug("Task Scheduler Stream Resources Cleaned...");
				lock.acquire();
				if (Objects.nonNull(DataSamudayaCacheManager.get())) {
					DataSamudayaCacheManager.get().close();
					DataSamudayaCacheManager.put(null);
				}
				lock.release();
				log.debug("Faltering the stream...");
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		SQLServer.start();
		PigQueryServer.start();
		JShellServer.startJShell();
	}

	public static void startTaskScheduler(ZookeeperOperations zo, CountDownLatch cdl) throws Exception {
		var cdlmr = new CountDownLatch(1);
		var zookeeperid = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOST))
				+ DataSamudayaConstants.UNDERSCORE + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT);
		zo.leaderElectionScheduler(zookeeperid, new LeaderLatchListener(){

			@Override
			public void isLeader() {
				log.debug("Scheduler Node {} elected as leader", zookeeperid);
				try {
					zo.setLeader(zookeeperid.getBytes());
					cdlmr.countDown();
				} catch (Exception e) {
				}
			}

			@Override
			public void notLeader() {
			}

		});
		log.debug("Scheduler Waiting to elect as a leader...");
		cdlmr.await();
		var su = new ServerUtils();
		su.init(Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_WEB_PORT)),
				new TaskSchedulerWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
				+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new SummaryWebServlet(),
				DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.SUMMARY + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
		su.start();
		SQLServerMR.start();
		var es = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("TaskScheduler-",0).factory());
		var essingle = Executors.newThreadPerTaskExecutor(Thread.ofVirtual().name("TaskSchedulerSingle-",0).factory());
		var ss = new ServerSocket(
				Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT)));
		essingle.execute(() -> {
			while (true) {
				try {
					var s = ss.accept();
					var bytesl = new ArrayList<byte[]>();

					var in = new DataInputStream(s.getInputStream());
					while (true) {
						var len = in.readInt();
						byte buffer[] = new byte[len]; // this could be reused !
						while (len > 0) {
							len -= in.read(buffer, buffer.length - len, len);
						}
						// skipped: check for stream close
						Object obj = Utils.getKryo().readClassAndObject(new Input(buffer));
						if (obj instanceof Integer brkintval && brkintval == -1) {
							break;
						}
						bytesl.add((byte[]) obj);
					}
					String[] arguments = null;
					if (bytesl.size() > 2) {
						var totalargs = bytesl.size();
						arguments = new String[totalargs - 1];
						for (var index = 2;index < totalargs;index++) {
							arguments[index - 2] = new String(bytesl.get(index));
						}
					}
					es.execute(new TaskScheduler(bytesl.get(0), arguments, s, new String(bytesl.get(1))));

				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			}
		});
		Utils.addShutdownHook(() -> {
			try {
				log.debug(STOPPINGANDCLOSECONNECTION);
				log.debug("Destroying...");
				es.shutdown();
				essingle.shutdown();
				su.stop();
				su.destroy();
				cdl.countDown();
				log.debug("Task Scheduler Resources Cleaned...");
				lock.acquire();
				if (Objects.nonNull(DataSamudayaCacheManager.get())) {
					DataSamudayaCacheManager.get().close();
					DataSamudayaCacheManager.put(null);
				}
				lock.release();
				log.debug("Halting...");
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
	}
}
