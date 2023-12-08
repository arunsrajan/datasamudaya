package com.github.datasamudaya.common.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hdfs.MiniDFSCluster;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Utility class for creating the hadoop hdfs cluster for testing.
 * @author arun
 *
 */
public class HadoopTestUtilities {

  private HadoopTestUtilities() {}

	private static final Logger log = LoggerFactory.getLogger(HadoopTestUtilities.class);
  
  /**
   * Initialize and start hdfs cluster for the given parameters port, httpport and 
   * number of nodes to create.
   * @param port
   * @param httpport
   * @param numnodes
   * @return hdfs cluster object.
   * @throws Exception
   */
  public static MiniDFSCluster initHdfsCluster(int port, int httpport, int numnodes) {
	  try {
		  	Configuration conf = new Configuration();
	        conf.set("dfs.replication", "1");
	        conf.set("dfs.permissions.enabled", "false");

	        MiniDFSCluster.Builder builder = new MiniDFSCluster.Builder(conf);
	        builder.numDataNodes(numnodes);
	        builder.nameNodeHttpPort(httpport);
	        builder.nameNodePort(port);
	        builder.format(true);

	        return builder.build();
	
	  }
	  catch(Exception ex) {
		  log.error("Reusing the hadoop node for testing");
	  }
    return null;
  }

}
