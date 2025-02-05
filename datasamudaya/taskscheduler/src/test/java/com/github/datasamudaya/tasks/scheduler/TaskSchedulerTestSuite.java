package com.github.datasamudaya.tasks.scheduler;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;
import org.junit.runners.Suite.SuiteClasses;

@RunWith(Suite.class)
@SuiteClasses({DataSamudayaJobBuilderTest.class, MapReduceSqlBuilderTest.class, MapReduceSqlBuilderIgniteTest.class, JobConfigurationBuilderTest.class})
public class TaskSchedulerTestSuite {

}
