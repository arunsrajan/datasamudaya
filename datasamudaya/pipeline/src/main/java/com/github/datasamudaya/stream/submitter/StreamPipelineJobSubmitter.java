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
package com.github.datasamudaya.stream.submitter;

import static java.util.Objects.nonNull;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.net.URL;
import java.util.Objects;
import java.util.Random;

import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.Options;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.apache.hadoop.fs.FsUrlStreamHandlerFactory;
import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;

/**
 * Submit the stream pipelining API jobs.
 * @author Arun
 */
public class StreamPipelineJobSubmitter {

	static Logger log = Logger.getLogger(StreamPipelineJobSubmitter.class);

	/**
	 * Main method for sumbitting the MR jobs.
	 * 
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
		org.burningwave.core.assembler.StaticComponentContainer.Modules.exportAllToAll();
		try (var zo = new ZookeeperOperations()) {
			zo.connect();
			var hostport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HOSTPORT);
			var taskscheduler = (String) zo.getStreamSchedulerMaster();
			if (hostport != null || !Objects.isNull(taskscheduler)) {
				String currenttaskscheduler;
				// For docker container or kubernetes pods.
				if (hostport != null) {
					currenttaskscheduler = hostport;
				}
				// If not, obtain schedulers host port from zookeeper.
				else {
					var rand = new Random(System.currentTimeMillis());
					currenttaskscheduler = taskscheduler;
				}
				log.info("Adopting job scheduler for host with port: " + currenttaskscheduler);
				var mrjarpath = args[0];
				var ts = currenttaskscheduler.split(DataSamudayaConstants.UNDERSCORE);
				writeToTaskScheduler(ts, mrjarpath, args);
			}
		} catch (Exception ex) {
			log.error("Exception in submit Jar to Task Scheduler", ex);
		}
	}

	/**
	 * Submit the job to task scheduler.
	 * @param ts
	 * @param mrjarpath
	 * @param args
	 */
	public static void writeToTaskScheduler(String[] ts, String mrjarpath, String[] args) {
		try (var s = new Socket(ts[0], Integer.parseInt(ts[1]));
				var is = s.getInputStream();
				var os = s.getOutputStream();
				var baos = new ByteArrayOutputStream();
				var fisjarpath = new FileInputStream(mrjarpath);
				var br = new BufferedReader(new InputStreamReader(is));) {
			int ch;
			while ((ch = fisjarpath.read()) != -1) {
				baos.write(ch);
			}
			// File bytes sent from localfile system to scheduler.
			Utils.writeDataStream(os, baos.toByteArray());
			// File name is sent to scheduler.
			Utils.writeDataStream(os, new File(mrjarpath).getName().getBytes());
			if (args.length > 1) {
				for (var argsindex = 1; argsindex < args.length; argsindex++) {
					var arg = args[argsindex];
					log.info("Dispatching arguments to application: " + arg);
					Utils.writeDataStream(os, arg.getBytes());
				}
			}
			Utils.writeInt(os, -1);
			// Wait for tasks to get completed.
			while (true) {
				var messagetasksscheduler = (String) br.readLine();
				if (nonNull(messagetasksscheduler)) {
					log.info(messagetasksscheduler);
					if (messagetasksscheduler.trim().contains("quit")) {
						break;
					}
				}
			}
		} catch (Exception ex) {
			log.error("Exception in submit Jar to Task Scheduler", ex);
		}
	}

}
