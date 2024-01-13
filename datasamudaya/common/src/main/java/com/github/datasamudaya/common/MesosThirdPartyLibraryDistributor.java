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

import java.net.InetSocketAddress;
import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.HttpConnectionFactory;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;

/**
 * Jetty server to obtain files from scheduler daemon by executors
 * to download and parse the properties  
 * @author Administrator
 *
 */
public class MesosThirdPartyLibraryDistributor {
	private final String dir;
	private Server server;

	public MesosThirdPartyLibraryDistributor(String dir) {
		this.dir = dir;
	}

	/**
	 * Start the server daemon
	 * @return
	 * @throws Throwable
	 */
	public int start() throws Throwable {
		server = new Server(new InetSocketAddress(DataSamudayaConstants.THISHOST, 0));

		server.getConnectors()[0].getConnectionFactory(HttpConnectionFactory.class);
		server.setHandler(new HttpRequestHandler(dir));

		server.start();

		Connector[] port = server.getConnectors();
		return ((ServerConnector) port[0]).getLocalPort();
	}

	/**
	 * Stop the server
	 * @throws Throwable
	 */
	public void stop() throws Throwable {
		if (server != null) {
			server.stop();
			server.destroy();
		}
	}
}
