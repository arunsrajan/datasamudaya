/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.io.IOException;

import javax.servlet.Servlet;
import javax.servlet.http.HttpServlet;

import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.server.ServerConnector;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.github.datasamudaya.common.utils.Utils;

/**
 * 
 * @author Arun Utility server for viewing, downloading the output from the data replicator.
 */
public class ServerUtils implements ServerUtilsMBean {

	static Logger log = LoggerFactory.getLogger(ServerUtils.class);

	Server server;

	/**
	* Initialize the server.
	*/
	@Override
	public void init(Object... config) throws Exception {
		if (config == null || config.length % 2 == 0 || config.length == 1) {
			throw new Exception("Server requires Port and atleast one servlet and url to access");
		} else if (!(config[0] instanceof Integer)) {
			throw new Exception("Configuration port must be integer");
		}
		var port = (Integer) config[0];
		// Create the server object.
		server = new Server(port);
		var context = new ServletContextHandler(ServletContextHandler.SESSIONS);
		context.setContextPath(DataSamudayaConstants.FORWARD_SLASH);
		server.setHandler(context);
		for (var conf = 1;conf < config.length;conf += 2) {
			if (!(config[conf] instanceof HttpServlet)) {
				throw new Exception(config[conf] + " which is of type " + config[conf].getClass().getName()
						+ " must be instance of servlet javax.servlet.http.HttpServlet");
			} else if (!(config[conf + 1] instanceof String)) {
				throw new Exception(
						"Path must be Url path of servlet " + config[conf].getClass().getName());
			}
			log.info("Configuring the servlet to receive the request.");
			context.addServlet(new ServletHolder((Servlet) config[conf]), (String) config[conf + 1]);
		}
	}

	/**
	* Start the server.
	*/
	@Override
	public void start() throws Exception {
		while (true) {
			try {
				if (server != null) {
					log.info("In ServerUtils start method starting...");
					server.start();
					log.info("In ServerUtils start method exiting...");
					return;
				}
			} catch (IOException ex) {
				log.error(DataSamudayaConstants.EMPTY, ex);
				int port = Utils.getRandomPort();
				((ServerConnector) (server.getConnectors()[0])).setPort(port);
			}
		}

	}

	/**
	* Stop the server.
	*/
	@Override
	public void stop() throws Exception {
		if (server != null) {
			server.stop();
		}
	}

	/**
	* Destroy the server.
	*/
	@Override
	public void destroy() throws Exception {
		if (server != null) {
			server.destroy();
		}

	}

}
