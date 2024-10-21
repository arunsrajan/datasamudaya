package com.github.datasamudaya.common;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

public class HadoopTestUtilitiesTest {

	@Test
	public void startHDFSCluster() throws Exception {
		MiniDFSCluster cluster = HadoopTestUtilities.initHdfsCluster(9001, 9880, 2);
		cluster.shutdown();
	}

}
