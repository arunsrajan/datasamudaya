package com.github.datasamudaya.common;

import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.junit.Test;

import com.github.datasamudaya.common.utils.HadoopTestUtilities;

public class HadoopTestUtilitiesTest {
	
	@Test
	public void startHDFSCluster() throws Exception {
		MiniDFSCluster cluster = HadoopTestUtilities.initHdfsCluster(9000, 9870, 2);
		cluster.shutdown();
	}

}
