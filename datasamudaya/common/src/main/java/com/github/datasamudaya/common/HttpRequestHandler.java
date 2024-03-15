/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.eclipse.jetty.server.Request;
import org.eclipse.jetty.server.handler.AbstractHandler;

/**
 * The file download Http request handler for mesos executor to download the required files like jar
 * or property files.
 * 
 * @author Arun
 *
 */
public class HttpRequestHandler extends AbstractHandler {

	private static final Logger log = Logger.getLogger(HttpRequestHandler.class);

	private final String dir;

	HttpRequestHandler(String dir) {
		this.dir = dir;
	}

	public void handle(String target, Request baseRequest, HttpServletRequest request,
			HttpServletResponse response) throws IOException {
		var relativePath = request.getPathInfo();
		var filePath = dir + relativePath;
		var downloadFile = new File(filePath);
		try (var inStream = new FileInputStream(downloadFile);
        var outStream = response.getOutputStream();) {


			// if you want to use a relative path to context root:

			log.debug("relativePath = " + relativePath);

			var mimeType = "application/octet-stream";

			log.debug("MIME type: " + mimeType);

			// modifies response
			response.setContentType(mimeType);
			response.setContentLength((int) downloadFile.length());

			// forces download
			var headerKey = "Content-Disposition";
			var headerValue = "attachment; filename=\"%s\"".formatted(downloadFile.getName());
			response.setHeader(headerKey, headerValue);

			// obtains response's output stream


			var buffer = new byte[4096];
			var bytesRead = -1;

			while ((bytesRead = inStream.read(buffer)) != -1) {
				outStream.write(buffer, 0, bytesRead);
			}

			inStream.close();
			outStream.close();

			baseRequest.setHandled(true);
		} catch (Exception ex) {
			log.error("Not able to upload file: ", ex);
		}
	}
}
