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

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintWriter;
import java.net.Socket;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.Set;

import org.apache.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.DataSamudayaProperties;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.stream.Pipeline;

/**
 * 
 * @author Arun
 * The stream pipelining API task scheduler thread to execut the MR jar file.
 */
public class StreamPipelineTaskScheduler implements Runnable {
	private static Logger log = Logger.getLogger(StreamPipelineTaskScheduler.class);
	private byte[] mrjar;
	private Socket tss;
	private String[] args;
	String filename;

	public StreamPipelineTaskScheduler(String filename, byte[] mrjar, String[] args,
			Socket tss) {
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
		try {
			//ClassLoader to load the jar file.
			ClassLoader ctxcl = Thread.currentThread().getContextClassLoader();
			var clsloader = DataSamudayaMapReducePhaseClassLoader.newInstance(mrjar, ctxcl);
			Thread.currentThread().setContextClassLoader(clsloader);
			var ismesos = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS));
			var isyarn = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN));
			//If not mesos and yarn load the jar by invoking socket of the task executors.
			if (ismesos || isyarn) {
				new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
				var fos = new FileOutputStream(DataSamudayaConstants.LOCAL_FS_APPJRPATH + filename);
				fos.write(mrjar);
				fos.close();

			}
			//Get the main class to execute.
			var mainclass = args[0];
			var main = Class.forName(mainclass, true, clsloader);
			Thread.currentThread().setContextClassLoader(clsloader);
			if (args == null) {
				args = new String[]{};
			} else {
				args = Arrays.copyOfRange(args, 1, args.length);
			}
			//Invoke the runPipeline method via reflection.
			var pipelineconfig = new PipelineConfig();
			pipelineconfig.setJar(mrjar);
			pipelineconfig.setClsloader(clsloader);
			Set<Class<?>> classes = new LinkedHashSet<>();
			pipelineconfig.setCustomclasses(classes);
			classes.add(main);
			pipelineconfig.setOutput(tss.getOutputStream());
			var pipeline = (Pipeline) main.getDeclaredConstructor().newInstance();
			pipelineconfig.setJobname(main.getSimpleName());
			pipeline.runPipeline(args, pipelineconfig);
			message = "Successfully Completed executing the Job from main class " + mainclass;
			Utils.writeToOstream(tss.getOutputStream(), message);
		}
		catch (Throwable ex) {
			log.error("Job execution Error, See cause below \n", ex);
			try (var baos = new ByteArrayOutputStream();) {
				var failuremessage = new PrintWriter(baos, true, StandardCharsets.UTF_8);
				ex.printStackTrace(failuremessage);
				Utils.writeToOstream(tss.getOutputStream(), new String(baos.toByteArray()));
			} catch (Exception e) {
				log.error("Message Send Failed for Task Failed: ", e);
			}
		}
		finally {
			try {
				Utils.writeToOstream(tss.getOutputStream(), "quit");
				tss.close();
			} catch (Exception ex) {
				log.error("Socket Stream close error, See cause below \n", ex);
			}
		}

	}
}
