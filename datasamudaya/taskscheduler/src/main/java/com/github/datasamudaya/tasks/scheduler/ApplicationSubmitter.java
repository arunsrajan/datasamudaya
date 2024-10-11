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
package com.github.datasamudaya.tasks.scheduler;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.util.Objects;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;
import org.burningwave.core.assembler.StaticComponentContainer;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

/**
 * Map Reduce application submitter.
 * @author arun
 *
 */
public class ApplicationSubmitter {

	static Logger log = Logger.getLogger(ApplicationSubmitter.class);

	public static void main(String[] args) throws Exception {
		URL.setURLStreamHandlerFactory(new FsUrlStreamHandlerFactory());
		String datasamudayahome = System.getenv(DataSamudayaConstants.DATASAMUDAYA_HOME);
		var options = new Options();

		options.addOption(DataSamudayaConstants.JAR, true, DataSamudayaConstants.MRJARREQUIRED);
		options.addOption(DataSamudayaConstants.ARGS, true, DataSamudayaConstants.ARGUEMENTSOPTIONAL);
		var parser = new DefaultParser();
		var cmd = parser.parse(options, args);

		String jarpath = null;
		String[] argue = null;
		if (cmd.hasOption(DataSamudayaConstants.JAR)) {
			jarpath = cmd.getOptionValue(DataSamudayaConstants.JAR);
		} else {
			var formatter = new HelpFormatter();
			formatter.printHelp(DataSamudayaConstants.ANTFORMATTER, options);
			return;
		}

		if (cmd.hasOption(DataSamudayaConstants.ARGS)) {
			argue = cmd.getOptionValue(DataSamudayaConstants.ARGS).split(" ");
		}
		String config = null;
		if (cmd.hasOption(DataSamudayaConstants.CONF)) {
			config = cmd.getOptionValue(DataSamudayaConstants.CONF);
			Utils.initializeProperties(DataSamudayaConstants.EMPTY, config);
		} else {
			Utils.initializeProperties(datasamudayahome + DataSamudayaConstants.FORWARD_SLASH
					+ DataSamudayaConstants.DIST_CONFIG_FOLDER + DataSamudayaConstants.FORWARD_SLASH, DataSamudayaConstants.DATASAMUDAYA_PROPERTIES);
		}
		StaticComponentContainer.Modules.exportAllToAll();
		try (var zo = new ZookeeperOperations()) {
			zo.connect();
			var hostport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOSTPORT);
			var taskscheduler = zo.getMRSchedulerMaster();
			if (hostport != null || nonNull(taskscheduler) && !taskscheduler.isEmpty()) {
				String currenttaskscheduler;
				if (hostport != null) {
					currenttaskscheduler = hostport;
				} else {
					currenttaskscheduler = taskscheduler;
				}
				var ts = currenttaskscheduler.split(DataSamudayaConstants.UNDERSCORE);
				try (var s = new Socket(ts[0], Integer.parseInt(ts[1]));
					var is = s.getInputStream();
					var os = s.getOutputStream();
					var fis = new FileInputStream(jarpath);
					var baos = new ByteArrayOutputStream();) {
					int ch;
					while ((ch = fis.read()) != -1) {
						baos.write(ch);
					}
					baos.flush();

					Utils.writeDataStream(os, baos.toByteArray());
					Utils.writeDataStream(os, new File(jarpath).getName().getBytes());
					if (!Objects.isNull(argue)) {
						for (var arg :argue) {
							Utils.writeDataStream(os, arg.getBytes());
						}
					}
					Utils.writeInt(os, -1);
					try (var br = new BufferedReader(new InputStreamReader(is));) {
						while (true) {
							var messagetasksscheduler = (String) br.readLine();
							if ("quit".equals(messagetasksscheduler.trim())) {
								break;
							}
							log.debug(messagetasksscheduler);
						}
					}
				} catch (Exception ex) {
					log.error(DataSamudayaConstants.EMPTY, ex);
				}
			}
		}
		catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		}
	}

}
