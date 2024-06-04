/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.tasks.executor.web;

import java.io.IOException;
import java.util.Map;

import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;

import com.github.datasamudaya.common.DataSamudayaConstants;
import com.github.datasamudaya.common.DataSamudayaProperties;


/**
 * Node servlet 
 * @author Arun
 */
public class NodeWebServlet extends HttpServlet {
	Map<String, Map<String, Process>> containersipport;

	public NodeWebServlet(Map<String, Map<String, Process>> containersipport) {
		this.containersipport = containersipport;
	}

	private static final long serialVersionUID = 8713220540678338208L;
	private static final Logger log = Logger.getLogger(NodeWebServlet.class);

	protected void doGet(HttpServletRequest request, HttpServletResponse response)
			throws IOException {
		response.setContentType(DataSamudayaConstants.TEXTHTML);
		response.setStatus(HttpServletResponse.SC_OK);
		var writer = response.getWriter();
		String contextpath = request.getScheme() + "://" + request.getServerName() + DataSamudayaConstants.COLON
				+ request.getLocalPort();
		try {
			writer.println(("""
          <!DOCTYPE HTML>
          <html>
          <head>
          <link rel="stylesheet" href="%s/resources/jquery-ui.css">
          <script src="%s/resources/jquery-1.11.1.min.js"></script>
          <script src="%s/resources/jquery.canvasjs.min.js"></script>
          <script src="%s/resources/jquery-ui.js"></script>
          <script>
          $(function() {

          var dataPointsCpu = [];
          var dataPointsMemory = [];


          var optionsCpu = {
          	theme: "light2",
          	title: {
          		text: "Cpu Usage"
          	},
          	data: [{
          		type: "line",
          		dataPoints: dataPointsCpu
          	}]
          };
          $("#chartContainerCpu").CanvasJSChart(optionsCpu);


          var optionsMemory = {
          	theme: "light2",
          	title: {
          		text: "Memory Usage"
          	},
          	data: [{
          		type: "line",
          		dataPoints: dataPointsMemory
          	}]
          };
          $("#chartContainerMemory").CanvasJSChart(optionsMemory);


          updateData();

          // Initial Values
          var xValue = 0;
          var yValue = 100;
          var newDataCount = 6;

          function addData(data) {
          	{
          		//dataPoints.shift();
          		dataPointsCpu.push({x: xValue, y: parseInt(data[0])});
          		dataPointsMemory.push({x: xValue, y: parseInt(data[1])});
          		xValue++;
          	}

          	newDataCount = 1;

          	$("#chartContainerCpu").CanvasJSChart().render()
          	$("#chartContainerMemory").CanvasJSChart().render()
          	setTimeout(updateData, 1500);
          }

          function updateData() {
          	$.getJSON("%s/data", addData);
          }


          $( "#tabs" ).tabs();
          });
          </script>
          </head>
          <body>
          <H1>%s</H1>
          <div>
          <p>--------------------------------------------------------------------------</p>
          </div>
          <div id="tabs">
            <ul>
              <li><a href="#tabs-1">Cpu Usage</a></li>
              <li><a href="#tabs-2">Memory Usage</a></li>
            </ul>
            <div id="tabs-1">
              <div id="chartContainerCpu" style="height: 500px; width: 600px;"></div>
            </div>
            <div id="tabs-2">
              <div id="chartContainerMemory" style="height: 500px; width: 600px;"></div>
            </div>
          </div>""" + getIframe() + """
          </body>
          </html>
          					""").formatted(contextpath, contextpath, contextpath, contextpath, contextpath,
					request.getServerName() + DataSamudayaConstants.COLON + request.getLocalPort()));
		} catch (Exception ex) {
			log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
		}
	}

	public String getIframe() {
		StringBuilder containersiframe = new StringBuilder();
		containersiframe.append("""
        <H1>Containers</H1>
        <div>
        	<p>--------------------------------------------------------------------------</p>
        	</div>
        """);
		containersipport.keySet().stream()
				.flatMap(container -> containersipport.get(container).keySet().stream())
				.map(port -> (DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST)
						+ DataSamudayaConstants.UNDERSCORE + (Integer.parseInt(port) + DataSamudayaConstants.PORT_OFFSET))
						+ "<BR/><iframe src=\"http://"
						+ DataSamudayaProperties.get().getProperty(DataSamudayaConstants.TASKEXECUTOR_HOST) + DataSamudayaConstants.COLON
						+ (Integer.parseInt(port) + DataSamudayaConstants.PORT_OFFSET)
						+ "\" width=\"900px\" height=\"800px\" style=\"border:1px solid black;\">\r\n"
						+ "</iframe>")
				.forEach(iframe -> {
					containersiframe.append(iframe);
					containersiframe.append("<BR/>");
				});
		return containersiframe.toString();
	}
}
