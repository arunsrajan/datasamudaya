package com.github.datasamudaya.tasks.scheduler;

import java.io.DataInputStream;
import java.net.ServerSocket;
import java.net.URL;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.curator.framework.recipes.leader.LeaderLatchListener;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.esotericsoftware.kryo.io.Input;
import com.github.datasamudaya.common.CacheUtils;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.TaskSchedulerWebServlet;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.tasks.scheduler.sql.SQLServerMR;

/**
 * Task scheduler for map reduce application.
 * @author arun
 *
 */
public class TaskSchedulerRunner {

	static Logger log = LoggerFactory.getLogger(TaskSchedulerRunner.class);

	public static void main(String[] args) throws Exception {		
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
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
		var cdlmr = new CountDownLatch(1);
		var zookeeperid = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOST))
				+ DataSamudayaConstants.UNDERSCORE + DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT);
		
		var zo = new ZookeeperOperations();			
		zo.connect();
		zo.createSchedulersLeaderNode(DataSamudayaConstants.EMPTY.getBytes(), event -> {
			log.info("Node Created");
		});
		zo.watchNodes();
		zo.leaderElectionScheduler(zookeeperid, new LeaderLatchListener(){

			@Override
			public void isLeader() {
				log.info("Scheduler Node {} elected as leader", zookeeperid);
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
		log.info("Scheduler Waiting to elect as a leader...");
		cdlmr.await();
		
		String cacheid = DataSamudayaConstants.BLOCKCACHE;
		CacheUtils.initCache(cacheid, 
				DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CACHEDISKPATH,
		                DataSamudayaConstants.CACHEDISKPATH_DEFAULT) + DataSamudayaConstants.FORWARD_SLASH
			            + DataSamudayaConstants.CACHEBLOCKS);
		CacheUtils.initBlockMetadataCache(cacheid);
		var cdl = new CountDownLatch(1);
		var su = new ServerUtils();
		su.init(Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_WEB_PORT)),
				new TaskSchedulerWebServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
				new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES
						+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX);
		su.start();
		SQLServerMR.start();
		var es = Executors.newWorkStealingPool();
		var essingle = Executors.newSingleThreadExecutor();
		

		var ss = new ServerSocket(Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT)));
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
				log.debug("Destroying...");
				zo.close();
				es.shutdown();
				essingle.shutdown();
				su.stop();
				su.destroy();
				cdl.countDown();
				log.debug("Halting...");
			} catch (Exception e) {
				log.error(DataSamudayaConstants.EMPTY, e);
			}
		});
		String mrport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT);
		String mrwebport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_WEB_PORT);
		log.info("MapReduce scheduler kickoff at the ports[port={},webport={}]", mrport, mrwebport);
		cdl.await();
	}

}
