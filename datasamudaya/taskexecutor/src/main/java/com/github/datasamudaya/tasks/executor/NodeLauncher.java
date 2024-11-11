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

import java.net.URI;
import java.net.URL;
import java.rmi.registry.Registry;
import java.rmi.server.UnicastRemoteObject;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Collectors;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.PropertyConfigurator;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.NetworkUtil;
import com.github.datasamudaya.common.Resources;
import com.github.datasamudaya.common.ServerUtils;
import com.github.datasamudaya.common.StreamDataCruncher;
import com.github.datasamudaya.common.WebResourcesServlet;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.tasks.executor.web.NodeWebServlet;
import com.github.datasamudaya.tasks.executor.web.ResourcesMetricsServlet;

/**
 * The node launcher class.
 * @author arun
 *
 */
public class NodeLauncher {
	static Logger log = LoggerFactory.getLogger(NodeLauncher.class);
	static Registry server;
	static StreamDataCruncher stub;
	static StreamDataCruncher sdc;

	/**
	* Main class to start and run the node launcher.
	* @param args
	* @throws Exception
	*/
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
		var port = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
		try (var zo = new ZookeeperOperations();) {
			zo.connect();
			var host = NetworkUtil.getNetworkAddress(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST));
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
			var escontainer = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors(), Thread.ofVirtual().factory());

			var hdfs =
					FileSystem.get(new URI(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL,
							DataSamudayaConstants.HDFSNAMENODEURL)), new Configuration());
			var containerprocesses = new ConcurrentHashMap<String, Map<String, Process>>();
			var containeridthreads = new ConcurrentHashMap<String, Map<String, List<Thread>>>();
			var containeridports = new ConcurrentHashMap<String, List<Integer>>();
			var su = new ServerUtils();
			su.init(port + DataSamudayaConstants.PORT_OFFSET, new NodeWebServlet(containerprocesses),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX, new WebResourcesServlet(),
					DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.RESOURCES + DataSamudayaConstants.FORWARD_SLASH
							+ DataSamudayaConstants.ASTERIX,
					new ResourcesMetricsServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.DATA
					+ DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.ASTERIX,
					new WebResourcesServlet(), DataSamudayaConstants.FORWARD_SLASH + DataSamudayaConstants.FAVICON);
			su.start();

			sdc = new StreamDataCruncher() {
				public Object postObject(Object object) {
					try {
						if (object instanceof byte[] bytes) {
							object = Utils.convertBytesToObjectCompressed(bytes, null);
						}
						var container = new NodeRunner(DataSamudayaConstants.PROPLOADERCONFIGFOLDER, containerprocesses,
								hdfs, containeridthreads, containeridports, object, zo);
						Future<Object> containerallocated = escontainer.submit(container);
						Object obj = containerallocated.get();
						log.debug("Chamber reply: " + obj);
						return obj;
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
			server = Utils.getRPCRegistry(port, sdc, DataSamudayaConstants.EMPTY);
			log.debug("NodeLauncher kickoff at port {}.....",
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NODE_PORT));
			log.debug("Reckoning closedown lock...");
			var cdl = new CountDownLatch(1);
			Utils.addShutdownHook(() -> {
				try {
					containerprocesses
							.keySet().stream().map(containerprocesses::get).flatMap(mapproc -> mapproc.keySet()
							.stream().map(key -> mapproc.get(key)).collect(Collectors.toList()).stream())
							.forEach(proc -> {
								log.debug("Destroying the Container Process: " + proc);
								proc.destroy();
							});
					log.debug("Stopping and closes all the connections...");
					log.debug("Destroying...");
					hdfs.close();
					if (Objects.nonNull(server)) {
						UnicastRemoteObject.unexportObject(server, true);
					}
					if (nonNull(zo)) {
						zo.close();
					}
					cdl.countDown();
					Runtime.getRuntime().halt(0);
				} catch (Exception e) {
					log.debug("", e);
				}
			});
			cdl.await();
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error("Unable to start Node Manager due to ", ex);
		}
	}

}
