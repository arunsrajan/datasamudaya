/*
 * Copyright 2021 the original author or authors.
 * <p>
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 * <p>
 * https://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.datasamudaya.common.utils;

import java.util.concurrent.CountDownLatch;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.log4j.PropertyConfigurator;
import org.apache.zookeeper.server.ServerCnxn.DisconnectReason;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.burningwave.core.assembler.StaticComponentContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;

/**
 * Zookeeper Main class for starting the new zookeeper instance. 
 * @author arun
 *
 */
public class ZookeeperMain {
	static Logger log = LoggerFactory.getLogger(ZookeeperMain.class);

	public static void main(String[] args) throws Exception {
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
		var cdl = new CountDownLatch(1);
		var clientport = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_STANDALONE_CLIENTPORT,
				DataSamudayaConstants.ZOOKEEPER_STANDALONE_CLIENTPORT_DEFAULT));
		var numconnections = Integer
				.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS,
						DataSamudayaConstants.ZOOKEEPER_STANDALONE_NUMCONNECTIONS_DEFAULT));
		var ticktime = Integer.parseInt(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_STANDALONE_TICKTIME,
				DataSamudayaConstants.ZOOKEEPER_STANDALONE_TICKTIME_DEFAULT));
		try {
			ServerCnxnFactory scf = Utils.startZookeeperServer(clientport, numconnections, ticktime);
			Utils.addShutdownHook(() -> {
				cdl.countDown();
				log.info("Bumbling Zookeeper...");
				scf.closeAll(DisconnectReason.CONNECTION_CLOSE_FORCED);

			});
		} catch (Exception e) {
			log.error("Error in bumbling zookeeper", e);
		}
		cdl.await();
	}
}
