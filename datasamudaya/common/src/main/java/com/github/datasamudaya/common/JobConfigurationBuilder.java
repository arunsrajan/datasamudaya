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

/**
 * Builder class for building the JobConfiguration object. 
 * @author arun
 *
 */
public class JobConfigurationBuilder {
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
	String minmem;
	String maxmem;
	String gctype;
	String numberofcontainers;
	String isblocksuserdefined;
	String execmode;
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

	private JobConfigurationBuilder() {
		hdfsurl = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HDFSNAMENODEURL);
		tstempdir = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_TMP_DIR);
		tshost = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_HOST);
		tsport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PORT);
		zkport = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_HOSTPORT);
		zkretrydelay = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ZOOKEEPER_RETRYDELAY);
		tspingdelay = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_PINGDELAY);
		tsrescheduledelay = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_RESCHEDULEDELAY);
		tsinitialdelay = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_INITIALDELAY);
		tepingdelay = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_PINGDELAY);
		hdfs = Boolean.parseBoolean(DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_ISHDFS));
		blocksize = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_BLOCKSIZE);
		batchsize = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_BATCHSIZE);
		numofreducers = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_NUMREDUCERS);
		maxmem = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MAXMEMORY, DataSamudayaConstants.CONTAINER_MAXMEMORY_DEFAULT);
		minmem = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MINMEMORY, DataSamudayaConstants.CONTAINER_MINMEMORY_DEFAULT);
		gctype = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GCCONFIG, DataSamudayaConstants.GCCONFIG_DEFAULT);
		numberofcontainers = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS,
				DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT);
		isblocksuserdefined = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE, DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT);
		execmode = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.EXECMODE, DataSamudayaConstants.EXECMODE_DEFAULT);
		taskexeccount = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.EXECUTIONCOUNT, DataSamudayaConstants.EXECUTIONCOUNT_DEFAULT);
		ignitehp = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEHOSTPORT, DataSamudayaConstants.IGNITEHOSTPORT_DEFAULT);
		ignitemulticastgroup = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEMULTICASTGROUP, DataSamudayaConstants.IGNITEMULTICASTGROUP_DEFAULT);
		ignitebackup = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEBACKUP, DataSamudayaConstants.IGNITEBACKUP_DEFAULT);
		yarnrm = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.YARNRM, DataSamudayaConstants.YARNRM_DEFAULT);
		yarnscheduler = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.YARNSCHEDULER, DataSamudayaConstants.YARNSCHEDULER_DEFAULT);
		containeralloc = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CONTAINER_ALLOC, DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT);
		heappercentage = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT);
		implicitcontainerallocanumber = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT);
		implicitcontainercpu = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT);
		implicitcontainermemory = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT);
		implicitcontainermemorysize = DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT);
	}

	public static JobConfigurationBuilder newBuilder() {
		return new JobConfigurationBuilder();
	}

	public JobConfigurationBuilder setHdfsurl(String hdfsurl) {
		this.hdfsurl = hdfsurl;
		return this;
	}

	public JobConfigurationBuilder setTstempdir(String tstempdir) {
		this.tstempdir = tstempdir;
		return this;
	}

	public JobConfigurationBuilder setTshost(String tshost) {
		this.tshost = tshost;
		return this;
	}

	public JobConfigurationBuilder setTsport(String tsport) {
		this.tsport = tsport;
		return this;
	}

	public JobConfigurationBuilder setZkport(String zkport) {
		this.zkport = zkport;
		return this;
	}

	public JobConfigurationBuilder setZkretrydelay(String zkretrydelay) {
		this.zkretrydelay = zkretrydelay;
		return this;
	}

	public JobConfigurationBuilder setTspingdelay(String tspingdelay) {
		this.tspingdelay = tspingdelay;
		return this;
	}

	public JobConfigurationBuilder setTsrescheduledelay(String tsrescheduledelay) {
		this.tsrescheduledelay = tsrescheduledelay;
		return this;
	}

	public JobConfigurationBuilder setTsinitialdelay(String tsinitialdelay) {
		this.tsinitialdelay = tsinitialdelay;
		return this;
	}

	public JobConfigurationBuilder setTepingdelay(String tepingdelay) {
		this.tepingdelay = tepingdelay;
		return this;
	}

	public JobConfigurationBuilder setHdfs(Boolean hdfs) {
		this.hdfs = hdfs;
		return this;
	}

	public JobConfigurationBuilder setBlocksize(String blocksize) {
		this.blocksize = blocksize;
		return this;
	}

	public JobConfigurationBuilder setBatchsize(String batchsize) {
		this.batchsize = batchsize;
		return this;
	}

	public JobConfigurationBuilder setNumofreducers(String numofreducers) {
		this.numofreducers = numofreducers;
		return this;
	}

	public JobConfiguration build() {
		return new JobConfiguration(this);

	}

	public String getMinmem() {
		return minmem;
	}

	public JobConfigurationBuilder setMinmem(String minmem) {
		this.minmem = minmem;
		return this;
	}

	public String getMaxmem() {
		return maxmem;
	}

	public JobConfigurationBuilder setMaxmem(String maxmem) {
		this.maxmem = maxmem;
		return this;
	}

	public String getGctype() {
		return gctype;
	}

	public JobConfigurationBuilder setGctype(String gctype) {
		this.gctype = gctype;
		return this;
	}

	public String getNumberofcontainers() {
		return numberofcontainers;
	}


	public JobConfigurationBuilder setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
		return this;
	}

	public JobConfigurationBuilder setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
		return this;
	}

	public String getExecmode() {
		return execmode;
	}

	public JobConfigurationBuilder setExecmode(String execmode) {
		this.execmode = execmode;
		return this;
	}

	public JobConfigurationBuilder setTaskexeccount(String taskexeccount) {
		this.taskexeccount = taskexeccount;
		return this;
	}

	public JobConfigurationBuilder setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
		return this;
	}

	public JobConfigurationBuilder setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
		return this;
	}

	public JobConfigurationBuilder setYarnrm(String yarnrm) {
		this.yarnrm = yarnrm;
		return this;
	}

	public JobConfigurationBuilder setYarnscheduler(String yarnscheduler) {
		this.yarnscheduler = yarnscheduler;
		return this;
	}

	public JobConfigurationBuilder setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
		return this;
	}

	public JobConfigurationBuilder setHeappercentage(String heappercentage) {
		this.heappercentage = heappercentage;
		return this;
	}

	public JobConfigurationBuilder setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
		return this;
	}

	public JobConfigurationBuilder setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
		return this;
	}

	public JobConfigurationBuilder setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
		return this;
	}

	public JobConfigurationBuilder setImplicitcontainermemorysize(String implicitcontainermemorysize) {
		this.implicitcontainermemorysize = implicitcontainermemorysize;
		return this;
	}

	public JobConfigurationBuilder setUser(String user) {
		this.user = user;
		return this;
	}

	public JobConfigurationBuilder setIsuseglobalte(Boolean isuseglobalte) {
		this.isuseglobalte = isuseglobalte;
		return this;
	}
	
	public JobConfigurationBuilder setTeappid(String teappid) {
		this.teappid = teappid;
		return this;
	}

	public JobConfigurationBuilder setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
		return this;
	}
	
	
	
}
