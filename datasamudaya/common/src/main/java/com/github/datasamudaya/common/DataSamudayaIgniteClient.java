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

import static java.util.Objects.isNull;
import static java.util.Objects.nonNull;

import java.util.Collections;

import org.apache.ignite.Ignite;
import org.apache.ignite.Ignition;
import org.apache.ignite.cache.CacheAtomicityMode;
import org.apache.ignite.cache.CacheMode;
import org.apache.ignite.configuration.CacheConfiguration;
import org.apache.ignite.configuration.DeploymentMode;
import org.apache.ignite.configuration.IgniteConfiguration;
import org.apache.ignite.spi.discovery.tcp.TcpDiscoverySpi;
import org.apache.ignite.spi.discovery.tcp.ipfinder.vm.TcpDiscoveryVmIpFinder;

/**
 * Ignite client instance initialization
 * @author arun
 *
 */
public class DataSamudayaIgniteClient {

	private DataSamudayaIgniteClient() {
	}

	private static Ignite ignite;
	/**
	 * Obtain ignite client object for given configuration for streaming scheduler.
	 * @param pipelineconfig
	 * @return ignite object
	 */
	public static synchronized Ignite instance(PipelineConfig pipelineconfig) {
		if (isNull(ignite) || nonNull(ignite) && !ignite.active()) {
			IgniteConfiguration cfg = new IgniteConfiguration()
				    .setDiscoverySpi(new TcpDiscoverySpi()
				        .setIpFinder(new TcpDiscoveryVmIpFinder()
				            .setAddresses(Collections.singletonList(pipelineconfig.getIgnitehp()))))
				    .setDeploymentMode(DeploymentMode.CONTINUOUS)
				    .setClientMode(true)
				    .setPeerClassLoadingEnabled(true)
				    .setCacheConfiguration(new CacheConfiguration<>(DataSamudayaConstants.DATASAMUDAYACACHE)
				    		.setCacheMode(CacheMode.PARTITIONED).setAtomicityMode(CacheAtomicityMode.ATOMIC)
				    		.setBackups(Integer.parseInt(pipelineconfig.getIgnitebackup())));
			ignite = Ignition.start(cfg);
		}
		return ignite;
	}
	
	/**
	 * Obtain ignite client object for given configuration for map reduce scheduler.
	 * @param jobconf
	 * @return ignite client object
	 */
	public static synchronized Ignite instanceMR(JobConfiguration jobconf) {
		if (isNull(ignite) || nonNull(ignite) && !ignite.active()) {			
			IgniteConfiguration cfg = new IgniteConfiguration()
				    .setDiscoverySpi(new TcpDiscoverySpi()
				        .setIpFinder(new TcpDiscoveryVmIpFinder()
					            .setAddresses(Collections.singletonList(jobconf.getIgnitehp()))))
				    .setDeploymentMode(DeploymentMode.CONTINUOUS)
				    .setClientMode(true)
				    .setPeerClassLoadingEnabled(true)
				    .setCacheConfiguration(new CacheConfiguration<>(DataSamudayaConstants.DATASAMUDAYACACHE)
				    		.setCacheMode(CacheMode.PARTITIONED).setAtomicityMode(CacheAtomicityMode.ATOMIC)
				    		.setBackups(Integer.parseInt(jobconf.getIgnitebackup())));
			ignite = Ignition.start(cfg);
		}
		return ignite;
	}
	

}
