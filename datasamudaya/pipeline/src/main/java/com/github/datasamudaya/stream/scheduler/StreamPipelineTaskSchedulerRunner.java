package com.github.datasamudaya.stream.scheduler;

import java.io.DataInputStream;
import java.io.IOException;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.jgroups.JChannel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.DataSamudayaCacheManager;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.TaskSchedulerWebServlet;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.JShellServer;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.pig.PigQueryServer;
import com.github.datasamudaya.stream.sql.SQLServer;
import com.github.datasamudaya.stream.utils.PipelineGraphWebServlet;

/**
 * The task scheduler daemon process.
 * @author Arun
 */
public class StreamPipelineTaskSchedulerRunner {
	static Logger log = LoggerFactory.getLogger(StreamPipelineTaskSchedulerRunner.class);
	static ServerSocket ss;
	static ExecutorService esstream;
	static ExecutorService es;
	static JChannel channel;
	static Map<String, Job> jobidjobmap = new ConcurrentHashMap<>();
	static ClassLoader cl;

	/**
	 * Main method for running task scheduler daemon.
	 * @param args
	 * @throws Exception
	 */
	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		// Load log4j properties.
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
		PropertyConfigurator.configure(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
				+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.LOG4J_PROPERTIES);
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
		var zo = new ZookeeperOperations();
		zo.connect();
		zo.createSchedulersLeaderNode(DataSamudayaConstants.EMPTY.getBytes(), event -> {
			log.debug("Node Created");
		});
		zo.watchNodes();
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

		String cacheid = DataSamudayaConstants.BLOCKCACHE;
		CacheUtils.initCache(cacheid,
				DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
						DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
						+ DataSamudayaConstants.CACHEBLOCKS);
		CacheUtils.initBlockMetadataCache(cacheid);
		var cdl = new CountDownLatch(1);
		var lbq = new LinkedBlockingQueue<StreamPipelineTaskScheduler>(Integer.valueOf(DataSamudayaProperties
				.get().getProperty(DataSamudayaConstants.DATASAMUDAYAJOBQUEUE_SIZE, DataSamudayaConstants.DATASAMUDAYAJOBQUEUE_SIZE_DEFAULT)));

		var esstream = Executors.newFixedThreadPool(1, Thread.ofVirtual().factory());
		var es = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), Thread.ofVirtual().factory());
		var su = new ServerUtils();
		su.init(Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_WEB_PORT)),
				new TaskSchedulerWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
				+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON,
				new PipelineGraphWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.GRAPH);
		su.start();

		SQLServer.start();

		PigQueryServer.start();
		JShellServer.startJShell();
		var execkind = DataSamudayaProperties
				.get().getProperty(DataSamudayaConstants.EXEC_KIND, DataSamudayaConstants.EXEC_KIND_DEFAULT);
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
						// executors.
						var filename = new String(bytesl.get(1));
						log.debug("Queueing the Job Name: {}", filename);
						var spts = new StreamPipelineTaskScheduler(filename, bytesl.get(0),
								arguments, s);
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
				if (Objects.nonNull(DataSamudayaCacheManager.get())) {
					DataSamudayaCacheManager.get().close();
				}
				if (Objects.nonNull(zo)) {
					zo.close();
				}
				cdl.countDown();
				log.debug("Program terminated...");
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		String streamport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PORT);
		String streamwebport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_WEB_PORT);
		log.debug("Program kickoff amidst port Stream[port={},webport={}]", streamport, streamwebport);
		cdl.await();
	}

	public static void closeResources() {
		if (!Objects.isNull(es)) {
			es.shutdown();
			es = null;
		}
		if (!Objects.isNull(esstream)) {
			esstream.shutdown();
		}
		if (!Objects.isNull(ss)) {
			try {
				ss.close();
				ss = null;
			} catch (IOException e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		}

		if (!Objects.isNull(channel)) {
			channel.close();
			channel = null;
		}
	}
}
