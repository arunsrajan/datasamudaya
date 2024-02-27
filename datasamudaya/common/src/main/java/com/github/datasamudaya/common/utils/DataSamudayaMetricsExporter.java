package com.github.datasamudaya.common.utils;

import io.prometheus.client.Counter;

/**
 * This class gathers and exports the metrics of DataSamudaya to prometheus 
 * @author arun
 *
 */
public class DataSamudayaMetricsExporter {
	
	private DataSamudayaMetricsExporter() {}
	
	private static Counter numsqlqueries = Counter.build()
            .name("datasamudaya_sql_queries_total")
            .help("Total number of sql queries.")
            .register();
	
	
	private static Counter numpigqueries = Counter.build()
            .name("datasamudaya_pig_queries_total")
            .help("Total number of pig queries.")
            .register();
	
	private static Counter numpigqueriesdumpexecuted = Counter.build()
            .name("datasamudaya_pig_queries_dump_total")
            .help("Total number of pig queries dump executed.")
            .register();
	
	
	private static Counter numjobssubmitted = Counter.build()
            .name("datasamudaya_jobs_submitted_total")
            .help("Total number of job submitted.")
            .register();
	
	private static Counter numjobssubmittedsa = Counter.build()
            .name("datasamudaya_jobs_submitted_standalone_total")
            .help("Total number of job submitted standalone mode.")
            .register();
	
	private static Counter numjobssubmittedignite = Counter.build()
            .name("datasamudaya_jobs_submitted_ignite_total")
            .help("Total number of job submitted ignite mode.")
            .register();
	
	private static Counter numjobssubmittedyarn = Counter.build()
            .name("datasamudaya_jobs_submitted_yarn_total")
            .help("Total number of job submitted yarn mode.")
            .register();
	
	private static Counter numjobssubmittedjgroups = Counter.build()
            .name("datasamudaya_jobs_submitted_jgroups_total")
            .help("Total number of job submitted jgroups mode.")
            .register();
	
	private static Counter numjobssubmittedlocal = Counter.build()
            .name("datasamudaya_jobs_submitted_local_total")
            .help("Total number of job submitted local mode.")
            .register();
	
	private static Counter numtaskexecutorsallocated = Counter.build()
            .name("datasamudaya_task_executors_allocated_total")
            .help("Total number of task executors allocated total.")
            .register();
	
	private static Counter numtaskexecutorsdeallocated = Counter.build()
            .name("datasamudaya_task_executors_deallocated_total")
            .help("Total number of task executors deallocated total.")
            .register();
	
	
	public static Counter getNumberOfSqlQueriesCounter() {
		return numsqlqueries;
	}
	
	public static Counter getNumberOfPigQueriesCounter() {
		return numpigqueries;
	}
	
	public static Counter getNumberOfPigQueriesDumpExecutedCounter() {
		return numpigqueriesdumpexecuted;
	}
	
	public static Counter getNumberOfJobSubmittedCounter() {
		return numjobssubmitted;
	}
	
	public static Counter getNumberOfJobSubmittedStandaloneModeCounter() {
		return numjobssubmittedsa;
	}
	
	public static Counter getNumberOfJobSubmittedYarnModeCounter() {
		return numjobssubmittedyarn;
	}
	
	public static Counter getNumberOfJobSubmittedIgniteModeCounter() {
		return numjobssubmittedignite;
	}
	
	public static Counter getNumberOfJobSubmittedJgroupsModeCounter() {
		return numjobssubmittedjgroups;
	}
	
	public static Counter getNumberOfJobSubmittedLocalModeCounter() {
		return numjobssubmittedlocal;
	}
	
	public static Counter getNumberOfTaskExecutorsAllocatedCounter() {
		return numtaskexecutorsallocated;
	}
	
	public static Counter getNumberOfTaskExecutorsDeAllocatedCounter() {
		return numtaskexecutorsdeallocated;
	}
	
}
