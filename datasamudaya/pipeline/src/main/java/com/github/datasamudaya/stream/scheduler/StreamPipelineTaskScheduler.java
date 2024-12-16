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
package com.github.datasamudaya.stream.scheduler;

import static java.util.Objects.isNull;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.Pipeline;

/**
 * 
 * @author Arun The stream pipelining API task scheduler thread to execut the MR
 *         jar file.
 */
public class StreamPipelineTaskScheduler implements Runnable {
	private static final Logger log = LogManager.getLogger(StreamPipelineTaskScheduler.class);
	private final byte[] mrjar;
	private final Socket tss;
	private String[] args;
	String filename;

	public StreamPipelineTaskScheduler(String filename, byte[] mrjar, String[] args, Socket tss) {
		this.mrjar = mrjar;
		this.args = args;
		this.tss = tss;
		this.filename = filename;
	}

	/**
	 * This method executes the tasks from the job submitted.
	 */
	@Override
	public void run() {

		var message = "";
		ClassLoader ctxcl = Thread.currentThread().getContextClassLoader();
		try {
			// ClassLoader to load the jar file.
			var clsloader = DataSamudayaMapReducePhaseClassLoader.newInstance(mrjar, ctxcl);
			Thread.currentThread().setContextClassLoader(clsloader);
			var ismesos = Boolean.parseBoolean(
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS));
			var isyarn = Boolean.parseBoolean(
					DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN));
			// If not mesos and yarn load the jar by invoking socket of the task executors.
			if (ismesos || isyarn) {
				new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
				var fos = new FileOutputStream(DataSamudayaConstants.LOCAL_FS_APPJRPATH + filename);
				fos.write(mrjar);
				fos.close();

			}
			// Get the main class to execute.
			var mainclass = args[0];
			var cpuexecutors = Integer.parseInt(args[1]);
			var memoryexecutors = Integer.parseInt(args[2]);
			var numberofexecutors = Integer.parseInt(args[3]);
			var cpudriver = Integer.parseInt(args[4]);
			var memorydriver = Integer.parseInt(args[5]);
			var user = args[6];
			var isdriverrequired = Boolean.parseBoolean(args[7]);
			var main = Class.forName(mainclass, true, clsloader);
			Thread.currentThread().setContextClassLoader(clsloader);
			if (args.length > 8) {
				args = Arrays.copyOfRange(args, 8, args.length);
			} else {
				args = new String[] {};
			}
			// Invoke the runPipeline method via reflection.
			var pipelineconfig = new PipelineConfig();
			pipelineconfig.setJar(mrjar);
			pipelineconfig.setClsloader(clsloader);
			Set<Class<?>> classes = new LinkedHashSet<>();
			pipelineconfig.setCustomclasses(classes);
			classes.add(main);
			pipelineconfig.setOutput(tss.getOutputStream());
			var pipeline = (Pipeline) main.getDeclaredConstructor().newInstance();
			if (isNull(pipelineconfig.getJobname())) {
				pipelineconfig.setJobname(main.getSimpleName());
			}
			pipelineconfig.setUser(user);
			pipelineconfig.setCpudriver(cpudriver);
			pipelineconfig.setMemorydriver(memorydriver);
			pipelineconfig.setNumtaskexecutors(numberofexecutors);
			pipelineconfig.setCputaskexecutor(cpuexecutors);
			pipelineconfig.setMemorytaskexceutor(memoryexecutors);
			pipelineconfig.setIsremotescheduler(isdriverrequired);
			pipeline.runPipeline(args, pipelineconfig);
			message = "Successfully Completed executing the Job from main class " + mainclass;
			Utils.writeToOstream(tss.getOutputStream(), message);
		} catch (Throwable ex) {
			log.error("Job execution Error, See cause below \n", ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				Utils.writeToOstream(tss.getOutputStream(), new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		} finally {
			try {
				Utils.writeToOstream(tss.getOutputStream(), "quit");
				tss.close();
				Thread.currentThread().setContextClassLoader(ctxcl);
			} catch (Exception ex) {
				log.error("Socket Stream close error, See cause below \n", ex);
			}
		}

	}
}
