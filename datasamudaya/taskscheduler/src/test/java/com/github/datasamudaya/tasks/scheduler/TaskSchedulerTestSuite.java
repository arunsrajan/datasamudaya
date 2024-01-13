package com.github.datasamudaya.tasks.scheduler;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({MapReduceSqlBuilderTest.class, DataSamudayaJobBuilderTest.class, JobConfigurationBuilderTest.class})
public class TaskSchedulerTestSuite {

}
