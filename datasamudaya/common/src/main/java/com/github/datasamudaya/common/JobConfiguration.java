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
package com.github.datasamudaya.common;

import java.io.OutputStream;
import java.util.Objects;

/**
 * Map Reduce Job configuration
 * @author arun
 *
 */
public class JobConfiguration {
	private OutputStream output;
	String hdfsurl;
	String tstempdir;
	String tshost;
	String tsport;
	String zkport;
	String zkretrydelay;
	String tspingdelay;
	String tsrescheduledelay;
	String tsinitialdelay;
	String tepingdelay;
	Boolean hdfs;
	String blocksize;
	String batchsize;
	String numofreducers;
	byte[] mrjar;
	String minmem;
	String maxmem;
	String gctype;
	String numberofcontainers;
	String isblocksuserdefined;
	String execmode;
	String outputfolder;
	String taskexeccount;
	String ignitehp;
	String ignitemulticastgroup;
	String ignitebackup;
	String yarnrm;
	String yarnscheduler;
	String containeralloc;
	String heappercentage;
	String implicitcontainerallocanumber;
	String implicitcontainercpu;
	String implicitcontainermemory;
	String implicitcontainermemorysize;
	String user;
	Boolean isuseglobalte;
	String teappid;

	public JobConfiguration(JobConfigurationBuilder builder) {
		this.hdfsurl = builder.hdfsurl;
		this.tstempdir = builder.tstempdir;
		this.tshost = builder.tshost;
		this.tsport = builder.tsport;
		this.zkport = builder.zkport;
		this.zkretrydelay = builder.zkretrydelay;
		this.tspingdelay = builder.tspingdelay;
		this.tsrescheduledelay = builder.tsrescheduledelay;
		this.tsinitialdelay = builder.tsinitialdelay;
		this.tepingdelay = builder.tepingdelay;
		this.hdfs = builder.hdfs;
		this.blocksize = builder.blocksize;
		this.batchsize = builder.batchsize;
		this.numofreducers = builder.numofreducers;
		this.minmem = builder.minmem;
		this.maxmem = builder.maxmem;
		this.gctype = builder.gctype;
		this.numberofcontainers = builder.numberofcontainers;
		this.isblocksuserdefined = builder.isblocksuserdefined;
		this.execmode = builder.execmode;
		this.taskexeccount = builder.taskexeccount;
		this.ignitehp = builder.ignitehp;
		this.ignitebackup = builder.ignitebackup;
		this.ignitemulticastgroup = builder.ignitemulticastgroup;
		this.yarnrm = builder.yarnrm;
		this.yarnscheduler = builder.yarnscheduler;
		this.containeralloc = builder.containeralloc;
		this.heappercentage = builder.heappercentage;
		this.implicitcontainerallocanumber = builder.implicitcontainerallocanumber;
		this.implicitcontainercpu = builder.implicitcontainercpu;
		this.implicitcontainermemory = builder.implicitcontainermemory;
		this.implicitcontainermemorysize = builder.implicitcontainermemorysize;
		this.isuseglobalte = builder.isuseglobalte;
		this.user = builder.user;
		this.teappid = builder.teappid;
	}

	public String getHdfsurl() {
		return hdfsurl;
	}

	public String getTstempdir() {
		return tstempdir;
	}

	public String getTshost() {
		return tshost;
	}

	public String getTsport() {
		return tsport;
	}

	public String getZkport() {
		return zkport;
	}

	public String getZkretrydelay() {
		return zkretrydelay;
	}

	public String getTspingdelay() {
		return tspingdelay;
	}

	public String getTsrescheduledelay() {
		return tsrescheduledelay;
	}

	public String getTsinitialdelay() {
		return tsinitialdelay;
	}

	public String getTepingdelay() {
		return tepingdelay;
	}

	public Boolean getHdfs() {
		return hdfs;
	}

	public String getBlocksize() {
		return blocksize;
	}

	public String getBatchsize() {
		return batchsize;
	}

	public String getNumofreducers() {
		return numofreducers;
	}

	public void setNumofreducers(String numofreducers) {
		this.numofreducers = numofreducers;
	}

	public byte[] getMrjar() {
		return mrjar;
	}

	public void setMrjar(byte[] mrjar) {
		if (!Objects.isNull(this.mrjar)) {
			throw new UnsupportedOperationException();
		}
		this.mrjar = mrjar;
	}

	public String getMinmem() {
		return minmem;
	}

	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}

	public String getMaxmem() {
		return maxmem;
	}

	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}

	public String getGctype() {
		return gctype;
	}

	public void setGctype(String gctype) {
		this.gctype = gctype;
	}

	public String getNumberofcontainers() {
		return numberofcontainers;
	}

	public void setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public void setHdfsurl(String hdfsurl) {
		this.hdfsurl = hdfsurl;
	}

	public void setTstempdir(String tstempdir) {
		this.tstempdir = tstempdir;
	}

	public void setTshost(String tshost) {
		this.tshost = tshost;
	}

	public void setTsport(String tsport) {
		this.tsport = tsport;
	}

	public void setZkport(String zkport) {
		this.zkport = zkport;
	}

	public void setZkretrydelay(String zkretrydelay) {
		this.zkretrydelay = zkretrydelay;
	}

	public void setTspingdelay(String tspingdelay) {
		this.tspingdelay = tspingdelay;
	}

	public void setTsrescheduledelay(String tsrescheduledelay) {
		this.tsrescheduledelay = tsrescheduledelay;
	}

	public void setTsinitialdelay(String tsinitialdelay) {
		this.tsinitialdelay = tsinitialdelay;
	}

	public void setTepingdelay(String tepingdelay) {
		this.tepingdelay = tepingdelay;
	}

	public void setHdfs(Boolean hdfs) {
		this.hdfs = hdfs;
	}

	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}

	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}

	public OutputStream getOutput() {
		return output;
	}

	public void setOutput(OutputStream output) {
		this.output = output;
	}

	public String getIsblocksuserdefined() {
		return isblocksuserdefined;
	}

	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}

	public String getExecmode() {
		return execmode;
	}

	public void setExecmode(String execmode) {
		this.execmode = execmode;
	}

	public String getOutputfolder() {
		return outputfolder;
	}

	public void setOutputfolder(String outputfolder) {
		this.outputfolder = outputfolder;
	}

	public String getTaskexeccount() {
		return taskexeccount;
	}

	public void setTaskexeccount(String taskexeccount) {
		this.taskexeccount = taskexeccount;
	}

	public String getIgnitemulticastgroup() {
		return ignitemulticastgroup;
	}

	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}

	public String getIgnitebackup() {
		return ignitebackup;
	}

	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}

	public String getYarnrm() {
		return yarnrm;
	}

	public void setYarnrm(String yarnrm) {
		this.yarnrm = yarnrm;
	}

	public String getYarnscheduler() {
		return yarnscheduler;
	}

	public void setYarnscheduler(String yarnscheduler) {
		this.yarnscheduler = yarnscheduler;
	}

	public String getContaineralloc() {
		return containeralloc;
	}

	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}

	public String getHeappercentage() {
		return heappercentage;
	}

	public void setHeappercentage(String heappercentage) {
		this.heappercentage = heappercentage;
	}

	public String getImplicitcontainerallocanumber() {
		return implicitcontainerallocanumber;
	}

	public void setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
	}

	public String getImplicitcontainercpu() {
		return implicitcontainercpu;
	}

	public void setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
	}

	public String getImplicitcontainermemory() {
		return implicitcontainermemory;
	}

	public void setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
	}

	public String getImplicitcontainermemorysize() {
		return implicitcontainermemorysize;
	}

	public void setImplicitcontainermemorysize(String implicitcontainermemorysize) {
		this.implicitcontainermemorysize = implicitcontainermemorysize;
	}

	public String getUser() {
		return user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public Boolean isIsuseglobalte() {
		return isuseglobalte;
	}

	public void setIsuseglobalte(Boolean isuseglobalte) {
		this.isuseglobalte = isuseglobalte;
	}

	public String getTeappid() {
		return teappid;
	}

	public void setTeappid(String teappid) {
		this.teappid = teappid;
	}

	public String getIgnitehp() {
		return ignitehp;
	}

	public void setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
	}

}
