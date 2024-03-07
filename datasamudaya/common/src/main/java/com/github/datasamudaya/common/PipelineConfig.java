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
import java.io.PrintWriter;
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

import static java.util.Objects.nonNull;

/**
 * 
 * @author arun
 * The configuration for pipeline interfaces.
 */
public class PipelineConfig implements Serializable, Cloneable {
	private static final Logger log = LoggerFactory.getLogger(PipelineConfig.class);
	private static final long serialVersionUID = 1L;
	private transient OutputStream output;
	private transient OutputStream pigoutput;
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
	private transient ClassLoader clsloader;
	private String user;
	private String jobid;
	private String tejobid;
	private Boolean isremotescheduler;
	private String sqlpigquery;
	private boolean topersistcolumnar;
	private transient PrintWriter writer;

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
		if(nonNull(blocksize)) {
			return blocksize;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE, DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_BLOCKSIZE_DEFAULT;
	}

	public void setBlocksize(String blocksize) {
		this.blocksize = blocksize;
	}

	public String getPingdelay() {
		if(nonNull(pingdelay)) {
			return pingdelay;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_PINGDELAY_DEFAULT;
	}

	public void setPingdelay(String pingdelay) {
		this.pingdelay = pingdelay;
	}

	public String getRescheduledelay() {
		if(nonNull(rescheduledelay)){
			return rescheduledelay;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_RESCHEDULEDELAY_DEFAULT;
	}

	public void setRescheduledelay(String rescheduledelay) {
		this.rescheduledelay = rescheduledelay;
	}

	public String getInitialdelay() {
		if(nonNull(initialdelay)) {
			return initialdelay;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY, DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_INITIALDELAY_DEFAULT;
	}

	public void setInitialdelay(String initialdelay) {
		this.initialdelay = initialdelay;
	}

	public String getBatchsize() {
		if(nonNull(batchsize)) {
			return batchsize;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE, DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_BATCHSIZE_DEFAULT;
	}

	public void setBatchsize(String batchsize) {
		this.batchsize = batchsize;
	}

	public String getMesos() {
		if(nonNull(mesos)) {
			return mesos;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISMESOS_DEFAULT;
	}

	public void setMesos(String mesos) {
		this.mesos = mesos;
	}

	public String getMesosmaster() {
		if(nonNull(mesosmaster)) {
			return mesosmaster;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MESOS_MASTER, DataSamudayaConstants.MESOS_MASTER_DEFAULT) : DataSamudayaConstants.MESOS_MASTER_DEFAULT;
	}

	public void setMesosmaster(String mesosmaster) {
		this.mesosmaster = mesosmaster;
	}

	public String getYarn() {
		if(nonNull(yarn)) {
			return yarn;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISYARN_DEFAULT;
	}

	public void setYarn(String yarn) {
		this.yarn = yarn;
	}

	public String getLocal() {
		if(nonNull(local)) {
			return local;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISLOCAL_DEFAULT;
	}

	public void setLocal(String local) {
		this.local = local;
	}

	public String getJgroups() {
		if(nonNull(jgroups)) {
			return jgroups;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS, DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_ISJGROUPS_DEFAULT;
	}

	public void setJgroups(String jgroups) {
		this.jgroups = jgroups;
	}

	public String getRandomte() {
		if(nonNull(randomte)) {
			return randomte;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULER_RANDTE, DataSamudayaConstants.TASKSCHEDULER_RANDTE_DEFAULT) : DataSamudayaConstants.TASKSCHEDULER_RANDTE_DEFAULT;
	}

	public void setRandomte(String randomte) {
		this.randomte = randomte;
	}

	public String getMinmem() {
		if(nonNull(minmem)) {
			return minmem;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MINMEMORY, DataSamudayaConstants.CONTAINER_MINMEMORY_DEFAULT) : DataSamudayaConstants.CONTAINER_MINMEMORY_DEFAULT;
	}

	public void setMinmem(String minmem) {
		this.minmem = minmem;
	}

	public String getMaxmem() {
		if(nonNull(maxmem)) {
			return maxmem;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MAXMEMORY, DataSamudayaConstants.CONTAINER_MAXMEMORY_DEFAULT) : DataSamudayaConstants.CONTAINER_MAXMEMORY_DEFAULT;
	}

	public void setMaxmem(String maxmem) {
		this.maxmem = maxmem;
	}

	public String getGctype() {
		if(nonNull(gctype)) {
			return gctype;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.GCCONFIG, DataSamudayaConstants.GCCONFIG_DEFAULT) : DataSamudayaConstants.GCCONFIG_DEFAULT;
	}

	public void setGctype(String gctype) {
		this.gctype = gctype;
	}

	public String getNumberofcontainers() {
		if(nonNull(numberofcontainers)) {
			return numberofcontainers;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.NUMBEROFCONTAINERS, DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT) : DataSamudayaConstants.NUMBEROFCONTAINERS_DEFAULT;
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
		if(nonNull(isblocksuserdefined)) {
			return isblocksuserdefined;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE, DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT) : DataSamudayaConstants.ISUSERDEFINEDBLOCKSIZE_DEFAULT;
	}

	public void setIsblocksuserdefined(String isblocksuserdefined) {
		this.isblocksuserdefined = isblocksuserdefined;
	}

	public String getMode() {
		if(nonNull(mode)) {
			return mode;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.MODE, DataSamudayaConstants.MODE_DEFAULT) : DataSamudayaConstants.MODE_DEFAULT;
	}

	public void setMode(String mode) {
		this.mode = mode;
	}

	public String getIgnitehp() {
		if(nonNull(ignitehp)) {
			return ignitehp;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEHOSTPORT, DataSamudayaConstants.IGNITEHOSTPORT_DEFAULT) : DataSamudayaConstants.IGNITEHOSTPORT_DEFAULT;
	}

	public void setIgnitehp(String ignitehp) {
		this.ignitehp = ignitehp;
	}

	public String getIgnitebackup() {
		if(nonNull(ignitebackup)) {
			return ignitebackup;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEBACKUP, DataSamudayaConstants.IGNITEBACKUP_DEFAULT) : DataSamudayaConstants.IGNITEBACKUP_DEFAULT;
	}

	public void setIgnitebackup(String ignitebackup) {
		this.ignitebackup = ignitebackup;
	}

	public String getIgnitemulticastgroup() {
		if(nonNull(ignitemulticastgroup)) {
			return ignitemulticastgroup;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IGNITEMULTICASTGROUP, DataSamudayaConstants.IGNITEMULTICASTGROUP_DEFAULT) : DataSamudayaConstants.IGNITEMULTICASTGROUP_DEFAULT;
	}

	public void setIgnitemulticastgroup(String ignitemulticastgroup) {
		this.ignitemulticastgroup = ignitemulticastgroup;
	}

	public String getExecutioncount() {
		if(nonNull(executioncount)) {
			return executioncount;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.EXECUTIONCOUNT, DataSamudayaConstants.EXECUTIONCOUNT_DEFAULT) : DataSamudayaConstants.EXECUTIONCOUNT_DEFAULT;
	}

	public void setExecutioncount(String executioncount) {
		this.executioncount = executioncount;
	}

	public String getTsshaenabled() {
		if(nonNull(tsshaenabled)) {
			return tsshaenabled;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED, DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT) : DataSamudayaConstants.TASKSCHEDULERSTREAM_HA_ENABLED_DEFAULT;
	}

	public void setTsshaenabled(String tsshaenabled) {
		this.tsshaenabled = tsshaenabled;
	}

	public STORAGE getStorage() {
		if(nonNull(storage)) {
			return storage;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.STORAGEPROP, DataSamudayaConstants.STORAGEPROP_DEFAULT).equals(DataSamudayaConstants.STORAGE.INMEMORY.name()) ? STORAGE.INMEMORY : STORAGE.DISK : STORAGE.INMEMORY;
	}

	public void setStorage(STORAGE storage) {
		this.storage = storage;
	}

	public String getContaineralloc() {
		if(nonNull(containeralloc)) {
			return containeralloc;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.CONTAINER_ALLOC, DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT) : DataSamudayaConstants.CONTAINER_ALLOC_DEFAULT;
	}

	public void setContaineralloc(String containeralloc) {
		this.containeralloc = containeralloc;
	}

	public String getHeappercent() {
		if(nonNull(heappercent)) {
			return heappercent;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.HEAP_PERCENTAGE, DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT) : DataSamudayaConstants.HEAP_PERCENTAGE_DEFAULT;
	}

	public Boolean getUseglobaltaskexecutors() {
		if(nonNull(useglobaltaskexecutors)) {
			return useglobaltaskexecutors;
		}
		return Boolean.parseBoolean(nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.USEGLOBALTASKEXECUTORS, DataSamudayaConstants.USEGLOBALTASKEXECUTORS_DEFAULT) : DataSamudayaConstants.USEGLOBALTASKEXECUTORS_DEFAULT);
	}


	public String getImplicitcontainerallocanumber() {
		if(nonNull(implicitcontainerallocanumber)) {
			return implicitcontainerallocanumber;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_NUMBER_DEFAULT;
	}

	public void setImplicitcontainerallocanumber(String implicitcontainerallocanumber) {
		this.implicitcontainerallocanumber = implicitcontainerallocanumber;
	}

	public String getImplicitcontainercpu() {
		if(nonNull(implicitcontainercpu)) {
			return implicitcontainercpu;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_CPU_DEFAULT;
	}

	public void setImplicitcontainercpu(String implicitcontainercpu) {
		this.implicitcontainercpu = implicitcontainercpu;
	}

	public String getImplicitcontainermemory() {
		if(nonNull(implicitcontainermemory)) {
			return implicitcontainermemory;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_DEFAULT;
	}

	public void setImplicitcontainermemory(String implicitcontainermemory) {
		this.implicitcontainermemory = implicitcontainermemory;
	}

	public String getImplicitcontainermemorysize() {
		if(nonNull(implicitcontainermemorysize)) {
			return implicitcontainermemorysize;
		}
		return nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE, DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT) : DataSamudayaConstants.IMPLICIT_CONTAINER_ALLOC_MEMORY_SIZE_DEFAULT;
	}
	
	

	public Boolean getIsremotescheduler() {
		if(nonNull(isremotescheduler)) {
			return isremotescheduler;
		}
		return Boolean.parseBoolean(nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.IS_REMOTE_SCHEDULER, DataSamudayaConstants.IS_REMOTE_SCHEDULER_DEFAULT) : DataSamudayaConstants.IS_REMOTE_SCHEDULER_DEFAULT);
	}

	public boolean isTopersistcolumnar() {
		if(nonNull(topersistcolumnar)) {
			return topersistcolumnar;
		}
		return Boolean.parseBoolean(nonNull(DataSamudayaProperties.get()) ? DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TOPERSISTYOSEGICOLUMNAR, DataSamudayaConstants.TOPERSISTYOSEGICOLUMNAR_DEFAULT) : DataSamudayaConstants.TOPERSISTYOSEGICOLUMNAR_DEFAULT);
	}

	public void setTopersistcolumnar(boolean topersistcolumnar) {
		this.topersistcolumnar = topersistcolumnar;
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

	public PrintWriter getWriter() {
		return writer;
	}

	public void setWriter(PrintWriter writer) {
		this.writer = writer;
	}
	
}
