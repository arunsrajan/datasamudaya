package com.github.datasamudaya.stream.scheduler;

import static java.util.Objects.nonNull;

import java.net.Socket;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.hadoop.shaded.org.apache.commons.collections.CollectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaMapReducePhaseClassLoader;
import com.github.datasamudaya.common.GlobalContainerAllocDealloc;
import com.github.datasamudaya.common.Job;
import com.github.datasamudaya.common.LoadJar;
import com.github.datasamudaya.common.PipelineConfig;
import com.github.datasamudaya.common.PipelineConstants;
import com.github.datasamudaya.common.DataSamudayaConstants.STORAGE;
import com.github.datasamudaya.common.exceptions.RpcRegistryException;
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
			String jobid = job.getId();
			if (pipelineconfig.getUseglobaltaskexecutors() && nonNull(pipelineconfig.getTejobid())) {
				jobid = pipelineconfig.getTejobid();
			}
			if (!Boolean.parseBoolean(pipelineconfig.getYarn())) {
				for (var lc : job.getLcs()) {
					List<Integer> ports = null;
					if (pipelineconfig.getUseglobaltaskexecutors() && pipelineconfig.getStorage() == STORAGE.COLUMNARSQL) {
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
									tehost + DataSamudayaConstants.UNDERSCORE + ports.get(index), loadjar, jobid));
						}
						index++;
					}
				}
			}
			var tes = zo.getTaskExectorsByJobId(jobid);
			if (pipelineconfig.getStorage() != STORAGE.COLUMNARSQL) {
				List<Integer> driverports = (List<Integer>) Utils.getResultObjectByInput(job.getDriver().getNodehostport(), job.getDriver(), DataSamudayaConstants.EMPTY);
				int index = 0;
				String tehost = job.getDriver().getNodehostport().split("_")[0];
				while (index < driverports.size()) {
					while (true) {
						try (Socket sock = new Socket(tehost, driverports.get(index))) {
							break;
						} catch (Exception ex) {
							Thread.sleep(200);
						}
					}
					if (nonNull(loadjar.getMrjar())) {
						log.debug("{}", Utils.getResultObjectByInput(
								tehost + DataSamudayaConstants.UNDERSCORE + driverports.get(index), loadjar, jobid));
					}
					index++;
				}
			}
			List<String> drivers = zo.getDriversByJobId(jobid);
			final String finaljobid = jobid;
			if (nonNull(loadjar.getMrjar())) {
				drivers.stream().forEach(driver -> {
					String[] driverhp = driver.split(DataSamudayaConstants.UNDERSCORE);
					try {
						log.debug("{}", Utils.getResultObjectByInput(
								driverhp[0] + DataSamudayaConstants.UNDERSCORE + driverhp[1], loadjar, finaljobid));
					} catch (RpcRegistryException e) {
						log.error(DataSamudayaConstants.EMPTY, e);
					}
				});
			}
			if (CollectionUtils.isEmpty(job.getTaskexecutors())) {
				job.setTaskexecutors(tes);
			}
			job.getTaskexecutors().addAll(0, drivers);
			taskexecutors = new LinkedHashSet<>();
			taskexecutors.addAll(drivers);
			taskexecutors.addAll(tes);
			while (taskexecutors.size() != job.getTaskexecutors().size()) {
				Thread.sleep(1000);
				taskexecutors.clear();
				drivers = zo.getDriversByJobId(jobid);
				taskexecutors.addAll(drivers);
				tes = zo.getTaskExectorsByJobId(jobid);				
				taskexecutors.addAll(tes);
			}
			job.setTaskexecutors(zo.getTaskExectorsByJobId(jobid));
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
			String jobid = job.getId();
			if (job.getPipelineconfig().getUseglobaltaskexecutors()) {
				jobid = job.getPipelineconfig().getTejobid();
			}
			;
			String choosente = getDriverNode(zo, jobid);
			job.getJm().setDriverhp(choosente);
			log.debug("Choosen Task Executor Host Port {}", choosente);
			Object output = null;
			if (nonNull(job.getPipelineconfig().getJar())) {
				output = Utils.getResultObjectByInput(choosente, job, jobid, DataSamudayaMapReducePhaseClassLoader.newInstance(job.getPipelineconfig().getJar(), Thread.currentThread().getContextClassLoader()));
			} else {
				if (Boolean.parseBoolean(job.getPipelineconfig().getYarn())) {
					job.getPipelineconfig().setYarn(Boolean.FALSE + DataSamudayaConstants.EMPTY);
				}
				output = Utils.getResultObjectByInput(choosente, job, jobid);
			}
			job.getJm().setJobcompletiontime(System.currentTimeMillis());
			Utils.writeToOstream(job.getPipelineconfig().getOutput(), "Concluded job in "
					+ ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			log.debug("Concluded job in "
					+ ((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0) + " seconds");
			job.getJm()
					.setTotaltimetaken((job.getJm().getJobcompletiontime() - job.getJm().getJobstarttime()) / 1000.0);
			Utils.writeToOstream(job.getPipelineconfig().getOutput(), "Job stats " + job.getJm());
			return output;
		} catch (Exception ex) {
			log.error(DataSamudayaConstants.EMPTY, ex);
		} finally {
			if (job.getPipelineconfig().getStorage() != STORAGE.COLUMNARSQL) {
				Utils.destroyContainers(job.getPipelineconfig().getUser(), job.getPipelineconfig().getTejobid());
			}
		}
		return null;
	}

	/**
	 * The function gets all the drivers from zookeeepr for given job id
	 * @param zo
	 * @param job
	 * @return driver host port
	 * @throws Exception
	 */
	protected String getDriverNode(ZookeeperOperations zo, String jobid) throws Exception {
		return zo.getDriversByJobId(jobid).stream().filter(hp -> taskexecutors.contains(hp)).findFirst().get();
	}

}
