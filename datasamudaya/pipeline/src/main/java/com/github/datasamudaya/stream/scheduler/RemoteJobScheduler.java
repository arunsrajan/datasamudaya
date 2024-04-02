package com.github.datasamudaya.stream.scheduler;

import static java.util.Objects.nonNull;

import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.utils.Utils;
import com.github.datasamudaya.common.utils.ZookeeperOperations;
import com.github.datasamudaya.stream.PipelineException;

/**
 * The class in which Job Scheduler is started in Executors.
 * @author arun
 *
 */
public class RemoteJobScheduler {

	private static final Logger log = LoggerFactory.getLogger(RemoteJobScheduler.class);
	Set<String> taskexecutors;

	/**
	 * 
	 * @param job
	 * @param pipelineconfig
	 * @throws PipelineException
	 */
	public void getTaskExecutorsHostPort(Job job, PipelineConfig pipelineconfig,
			ZookeeperOperations zo) throws PipelineException {
		try {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().acquire();
			var loadjar = new LoadJar();
			loadjar.setMrjar(pipelineconfig.getJar());
			if (nonNull(pipelineconfig.getCustomclasses()) && !pipelineconfig.getCustomclasses().isEmpty()) {
				loadjar.setClasses(pipelineconfig.getCustomclasses().stream().map(clz -> clz.getName())
						.collect(Collectors.toCollection(LinkedHashSet::new)));
			}
			for (var lc : job.getLcs()) {
				List<Integer> ports = null;
				if (pipelineconfig.getUseglobaltaskexecutors()) {
					ports = lc.getCla().getCr().stream().map(cr -> {
						return cr.getPort();
					}).collect(Collectors.toList());
				} else {
					ports = (List<Integer>) Utils.getResultObjectByInput(lc.getNodehostport(), lc, DataSamudayaConstants.EMPTY);
				}
				int index = 0;
				String tehost = lc.getNodehostport().split("_")[0];
				while (index < ports.size()) {
					while (true) {
						try (Socket sock = new Socket(tehost, ports.get(index))) {
							break;
						} catch (Exception ex) {
							Thread.sleep(200);
						}
					}
					if (nonNull(loadjar.getMrjar())) {
						log.debug("{}", Utils.getResultObjectByInput(
								tehost + DataSamudayaConstants.UNDERSCORE + ports.get(index), loadjar, job.getId()));
					}
					index++;
				}
			}
			String jobid = job.getId();
			if (pipelineconfig.getUseglobaltaskexecutors()) {
				jobid = pipelineconfig.getTejobid();
			}
			var tes = zo.getTaskExectorsByJobId(jobid);
			taskexecutors = new LinkedHashSet<>();
			taskexecutors.addAll(tes);
			while (taskexecutors.size() != job.getTaskexecutors().size()) {
				Thread.sleep(1000);
				tes = zo.getTaskExectorsByJobId(jobid);
				taskexecutors.clear();
				taskexecutors.addAll(tes);
			}
		} catch (InterruptedException e) {
			log.warn("Interrupted!", e);
			// Restore interrupted state...
			Thread.currentThread().interrupt();
		} catch (Exception ex) {
			log.error(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
			throw new PipelineException(PipelineConstants.JOBSCHEDULERCONTAINERERROR, ex);
		} finally {
			GlobalContainerAllocDealloc.getGlobalcontainerallocdeallocsem().release();
		}
	}

	/**
	 * Scheduler Job Remotely
	 * @param job
	 * @return
	 * @throws Exception 
	 */
	public Object scheduleJob(Job job) throws Exception {
		try (var zo = new ZookeeperOperations();) {
			zo.connect();
			getTaskExecutorsHostPort(job, job.getPipelineconfig(), zo);
			String choosente = taskexecutors.iterator().next();
			String jobid = job.getId();
			if (job.getPipelineconfig().getUseglobaltaskexecutors()) {
				jobid = job.getPipelineconfig().getTejobid();
			}
			Object output = null;
			if(nonNull(job.getPipelineconfig().getJar())) {
				output = Utils.getResultObjectByInput(choosente, job, jobid, DataSamudayaMapReducePhaseClassLoader.newInstance(job.getPipelineconfig().getJar(), Thread.currentThread().getContextClassLoader()));
			} else {
				output = Utils.getResultObjectByInput(choosente, job, jobid);
			}
			job.getJm().setJobcompletiontime(System.currentTimeMillis());
			Utils.writeToOstream(job.getPipelineconfig().getOutput(), "Concluded job in "
					+ ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			log.info("Concluded job in "
					+ ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			job.getJm()
					.setTotaltimetaken((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0);
			Utils.writeToOstream(job.getPipelineconfig().getOutput(), "Job stats " + job.getJm());
			return output;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {
			if (!job.getPipelineconfig().getUseglobaltaskexecutors()) {
				Utils.destroyTaskExecutors(job);
			}
		}
		return null;
	}


}
