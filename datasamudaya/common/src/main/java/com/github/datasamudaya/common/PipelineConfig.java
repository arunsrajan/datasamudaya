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

import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.Serializable;
import java.util.Objects;
import java.util.Set;
import java.util.jar.Attributes;
import java.util.jar.JarEntry;
import java.util.jar.JarOutputStream;
import java.util.jar.Manifest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;

/**
 * 
 * @author arun
 * The configuration for pipeline interfaces.
 */
public class PipelineConfig implements Serializable, Cloneable {
	private static Logger log = LoggerFactory.getLogger(PipelineConfig.class);
	private static final long serialVersionUID = 1L;
	transient private OutputStream  output, pigoutput;
	private String blocksize;
	private String isblocksuserdefined;
	private String pingdelay;
	private String rescheduledelay;
	private String initialdelay;
	private String batchsize;
	private String mesos;
	private String mesosmaster;
	private String yarn;
	private String local;
	private String jgroups;
	private String randomte;
	private String minmem;
	private String maxmem;
	private String gctype;
	private String numberofcontainers;
	private byte[] jar;
	private String mode;
	private String ignitehp;
	private String ignitebackup;
	private String ignitemulticastgroup;
	private String executioncount;
	private String tsshaenabled;
	private STORAGE storage;
	private String containeralloc;
	private String jobname;
	private String heappercent;
	private Boolean useglobaltaskexecutors;
	private String implicitcontainerallocanumber;
	private String implicitcontainercpu;
	private String implicitcontainermemory;
	private String implicitcontainermemorysize;
	private Set<Class<?>> customclasses;
	transient private ClassLoader clsloader;
	private String user;
	private String jobid;
	private String tejobid;
	private Boolean isremotescheduler;
	private String sqlpigquery;

	public void setOutput(OutputStream  output) {
		this.output = output;
	}

	public OutputStream getOutput() {
		return output;
	}
	
	public OutputStream getPigoutput() {
		return pigoutput;
	}

	public void setPigoutput(OutputStream pigoutput) {
		this.pigoutput = pigoutput;
	}

	public String getBlocksize() {
		return Objects.isNull(blocksize)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE, DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT : blocksize;
	}

	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}

	public String getPingdelay() {
		return Objects.isNull(pingdelay)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT : pingdelay;
	}

	public void setPingdelay(String pingdelay) {
		this.pingdelay = pingdelay;
	}

	public String getRescheduledelay() {
		return Objects.isNull(rescheduledelay)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT : rescheduledelay;
	}

	public void setRescheduledelay(String rescheduledelay) {
		this.rescheduledelay = rescheduledelay;
	}

	public String getInitialdelay() {
		return Objects.isNull(initialdelay)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT : initialdelay;
	}

	public void setInitialdelay(String initialdelay) {
		this.initialdelay = initialdelay;
	}

	public String getBatchsize() {
		return Objects.isNull(batchsize)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE, DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT : batchsize;
	}

	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}

	public String getMesos() {
		return Objects.isNull(mesos)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT : mesos;
	}

	public void setMesos(String mesos) {
		this.mesos = mesos;
	}

	public String getMesosmaster() {
		return Objects.isNull(mesosmaster)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MESOS_MASTER, DataSamudayaConstants.MESOS_MASTER_DEFAULT) : DataSamudayaConstants.MESOS_MASTER_DEFAULT : mesosmaster;
	}

	public void setMesosmaster(String mesosmaster) {
		this.mesosmaster = mesosmaster;
	}

	public String getYarn() {
		return Objects.isNull(yarn)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT : yarn;
	}

	public void setYarn(String yarn) {
		this.yarn = yarn;
	}

	public String getLocal() {
		return Objects.isNull(local)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT : local;
	}

	public void setLocal(String local) {
		this.local = local;
	}

	public String getJgroups() {
		return Objects.isNull(jgroups)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT : jgroups;
	}

	public void setJgroups(String jgroups) {
		this.jgroups = jgroups;
	}

	public String getRandomte() {
		return Objects.isNull(randomte)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_RANDTE, DataSamudayaConstants.TASKSCHEDULER_RANDTE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULER_RANDTE_DEFAULT : randomte;
	}

	public void setRandomte(String randomte) {
		this.randomte = randomte;
	}

	public String getMinmem() {
		return Objects.isNull(minmem)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MINMEMORY, DataSamudayaConstants.CONTAINER_MINMEMORY_DEFAULT) : DataSamudayaConstants.CONTAINER_MINMEMORY_DEFAULT : minmem;
	}

	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}

	public String getMaxmem() {
		return Objects.isNull(maxmem)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MAXMEMORY, DataSamudayaConstants.CONTAINER_MAXMEMORY_DEFAULT) : DataSamudayaConstants.CONTAINER_MAXMEMORY_DEFAULT : maxmem;
	}

	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}

	public String getGctype() {
		return Objects.isNull(gctype)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GCCONFIG, DataSamudayaConstants.GCCONFIG_DEFAULT) : DataSamudayaConstants.GCCONFIG_DEFAULT : gctype;
	}

	public void setGctype(String gctype) {
		this.gctype = gctype;
	}

	public String getNumberofcontainers() {
		return Objects.isNull(numberofcontainers)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS, DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT) : DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT : numberofcontainers;
	}

	public void setNumberofcontainers(String numberofcontainers) {
		this.numberofcontainers = numberofcontainers;
	}

	public byte[] getJar() {
		return jar;
	}

	public void setJar(byte[] jar) {
		if (!Objects.isNull(this.jar)) {
			throw new UnsupportedOperationException();
		}
		this.jar = jar;
	}
	
	public void setCustomclasses(Set<Class<?>> customclasses) {
		if (Objects.isNull(customclasses)) {
			throw new UnsupportedOperationException();
		}
		this.customclasses = customclasses;
	}

	public Set<Class<?>> getCustomclasses() {
		return customclasses;
	}

	public String getIsblocksusedefined() {
		return Objects.isNull(isblocksuserdefined)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE, DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT) : DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT : isblocksuserdefined;
	}

	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}

	public String getMode() {
		return Objects.isNull(mode)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MODE, DataSamudayaConstants.MODE_DEFAULT) : DataSamudayaConstants.MODE_DEFAULT : mode;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getIgnitehp() {
		return Objects.isNull(ignitehp)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEHOSTPORT, DataSamudayaConstants.IGNITEHOSTPORT_DEFAULT) : DataSamudayaConstants.IGNITEHOSTPORT_DEFAULT : ignitehp;
	}

	public void setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
	}

	public String getIgnitebackup() {
		return Objects.isNull(ignitebackup)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEBACKUP, DataSamudayaConstants.IGNITEBACKUP_DEFAULT) : DataSamudayaConstants.IGNITEBACKUP_DEFAULT : ignitebackup;
	}

	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}

	public String getIgnitemulticastgroup() {
		return Objects.isNull(ignitemulticastgroup)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEMULTICASTGROUP, DataSamudayaConstants.IGNITEMULTICASTGROUP_DEFAULT) : DataSamudayaConstants.IGNITEMULTICASTGROUP_DEFAULT : ignitemulticastgroup;
	}

	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}

	public String getExecutioncount() {
		return Objects.isNull(executioncount)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.EXECUTIONCOUNT, DataSamudayaConstants.EXECUTIONCOUNT_DEFAULT) : DataSamudayaConstants.EXECUTIONCOUNT_DEFAULT : executioncount;
	}

	public void setExecutioncount(String executioncount) {
		this.executioncount = executioncount;
	}

	public String getTsshaenabled() {
		return Objects.isNull(tsshaenabled)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED, DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT : tsshaenabled;
	}

	public void setTsshaenabled(String tsshaenabled) {
		this.tsshaenabled = tsshaenabled;
	}

	public STORAGE getStorage() {
		return Objects.isNull(storage)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.STORAGEPROP, DataSamudayaConstants.STORAGEPROP_DEFAULT).equals(DataSamudayaConstants.STORAGE.INMEMORY.name()) ? STORAGE.INMEMORY : STORAGE.DISK : STORAGE.INMEMORY : storage;
	}

	public void setStorage(STORAGE storage) {
		this.storage = storage;
	}

	public String getContaineralloc() {
		return Objects.isNull(containeralloc)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CONTAINER_ALLOC, DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT) : DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT : containeralloc;
	}

	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}

	public String getHeappercent() {
		return Objects.isNull(heappercent)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT) : DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT : heappercent;
	}

	public Boolean getUseglobaltaskexecutors() {
		return Objects.isNull(useglobaltaskexecutors)
				? Boolean.parseBoolean(Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.USEGLOBALTASKEXECUTORS, DataSamudayaConstants.USEGLOBALTASKEXECUTORS_DEFAULT) : DataSamudayaConstants.USEGLOBALTASKEXECUTORS_DEFAULT) : useglobaltaskexecutors;
	}


	public String getImplicitcontainerallocanumber() {
		return Objects.isNull(implicitcontainerallocanumber)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT : implicitcontainerallocanumber;
	}

	public void setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
	}

	public String getImplicitcontainercpu() {
		return Objects.isNull(implicitcontainercpu)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT : implicitcontainercpu;
	}

	public void setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
	}

	public String getImplicitcontainermemory() {
		return Objects.isNull(implicitcontainermemory)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT : implicitcontainermemory;
	}

	public void setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
	}

	public String getImplicitcontainermemorysize() {
		return Objects.isNull(implicitcontainermemorysize)
				? Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT : implicitcontainermemorysize;
	}
	
	

	public Boolean getIsremotescheduler() {
		return Objects.isNull(isremotescheduler)
				? Boolean.parseBoolean(Objects.nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IS_REMOTE_SCHEDULER, DataSamudayaConstants.IS_REMOTE_SCHEDULER_DEFAULT) : DataSamudayaConstants.IS_REMOTE_SCHEDULER_DEFAULT) : isremotescheduler;
	}

	public void setIsremotescheduler(Boolean isremotescheduler) {
		this.isremotescheduler = isremotescheduler;
	}

	public void setImplicitcontainermemorysize(String implicitcontainermemorysize) {
		this.implicitcontainermemorysize = implicitcontainermemorysize;
	}

	public void setUseglobaltaskexecutors(Boolean useglobaltaskexecutors) {
		this.useglobaltaskexecutors = useglobaltaskexecutors;
	}

	public void setHeappercent(String heappercent) {
		this.heappercent = heappercent;
	}

	public String getJobname() {
		return jobname;
	}

	public void setJobname(String jobname) {
		this.jobname = jobname;
	}

	public ClassLoader getClsloader() {
		return clsloader;
	}

	public void setClsloader(ClassLoader clsloader) {
		this.clsloader = clsloader;
	}
	
	public void setClsToJar(Class<?> cls) {
		var manifest = new Manifest();
	    manifest.getMainAttributes().put(Attributes.Name.MANIFEST_VERSION, "1.0");
		try(var jarBytes = new ByteArrayOutputStream();
				var jarOutputStream = new JarOutputStream(jarBytes, manifest);){
			
	        // Create a new JarEntry for the class file and add it to the jar file
	        var jarEntry = new JarEntry(cls.getName() + ".class");
	        jarOutputStream.putNextEntry(jarEntry);
	        
	        // Read the class file into a byte array and write it to the jar file
	        byte[] buffer = new byte[1024];
	        int bytesRead;
	        InputStream stream = this.getClass().getResourceAsStream(cls.getName() + ".class");
	        while ((bytesRead = stream.read(buffer)) != -1) {
	            jarOutputStream.write(buffer, 0, bytesRead);
	            jarOutputStream.flush();
	        }
	        jarOutputStream.closeEntry();
	        this.jar = jarBytes.toByteArray();
		} catch(Exception ex) {
			log.info(DataSamudayaConstants.EMPTY, ex);
		}
	}
	
	public String getUser() {
		return Objects.isNull(user)
				? DataSamudayaConstants.DEFAULT_CONTAINER_USER : user;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public String getJobid() {
		return jobid;
	}

	public void setJobid(String jobid) {
		this.jobid = jobid;
	}

	public String getTejobid() {
		return tejobid;
	}

	public void setTejobid(String tejobid) {
		this.tejobid = tejobid;
	}
	
	public String getSqlpigquery() {
		return sqlpigquery;
	}

	public void setSqlpigquery(String sqlpigquery) {
		this.sqlpigquery = sqlpigquery;
	}

	@Override
	public PipelineConfig clone() throws CloneNotSupportedException {
		return (PipelineConfig) super.clone();

	}

}
