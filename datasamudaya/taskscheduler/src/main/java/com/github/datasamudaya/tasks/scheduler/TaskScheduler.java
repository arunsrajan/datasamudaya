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

import java.io.File;
import java.io.FileOutputStream;
import java.net.Socket;
import java.util.Arrays;

import org.apache.log4j.Logger;

import com.github.datasamudaya.common.JobConfigurationBuilder;
import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.utils.Utils;

/**
 * Task scheduler for map reduce
 * @author arun
 *
 */
public class TaskScheduler implements Runnable {
	static Logger log = Logger.getLogger(TaskScheduler.class);
	byte[] mrjar;
	Socket tss;
	String[] args;
	String filename;

	public TaskScheduler(byte[] mrjar, String[] args, Socket tss, String filename) {
		this.mrjar = mrjar;
		this.args = args;
		this.tss = tss;
		this.filename = filename;
	}

	@Override
	public void run() {
		final ClassLoader loader = Thread.currentThread().getContextClassLoader();
		new File(DataSamudayaConstants.LOCAL_FS_APPJRPATH).mkdirs();
		try (var fos = new FileOutputStream(DataSamudayaConstants.LOCAL_FS_APPJRPATH + filename);) {
			fos.write(mrjar);
			var clsloader = DataSamudayaMapReducePhaseClassLoader.newInstance(mrjar, loader);
			Thread.currentThread().setContextClassLoader(clsloader);
			
			String[] argscopy;
			//Get the main class to execute.
			String mainclass;
			if (args == null) {
				argscopy = new String[]{};
				mainclass = "";
			} else {
				mainclass = args[0];
				argscopy = Arrays.copyOfRange(args, 1, args.length);
			}

			var main = Class.forName(mainclass, true, clsloader);
			var jc = JobConfigurationBuilder.newBuilder().build();
			jc.setMrjar(mrjar);
			
			try(var tssos = tss.getOutputStream();){
				jc.setOutput(tssos);
				var mrjob = (Application) main.getDeclaredConstructor().newInstance();
				mrjob.runMRJob(argscopy, jc);
				Utils.writeToOstream(tssos, "Successfully Completed executing the task " + mainclass);
				Utils.writeToOstream(tssos, "quit");
			}
		} catch (Throwable ex) {
			log.error("Exception in loading class:", ex);
		} finally {
			Thread.currentThread().setContextClassLoader(loader);
			try {
				tss.close();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}

	}
}
