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
import java.io.PrintWriter;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.lang.management.MemoryUsage;
import javax.management.Attribute;
import javax.management.AttributeList;
import javax.management.MBeanServer;
import javax.management.ObjectName;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;

import com.github.datasamudaya.common.DataSamudayaConstants;

/**
 * Resource Metrics servlet to display
 * @author Arun
 */
public class ResourcesMetricsServlet extends HttpServlet {

  private static final long serialVersionUID = 8713220540678338208L;
  private static Logger log = Logger.getLogger(ResourcesMetricsServlet.class);

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {

    response.setStatus(HttpServletResponse.SC_OK);
    response.setContentType("application/json");
    try (PrintWriter writer = response.getWriter();) {
      MemoryMXBean membean = ManagementFactory.getMemoryMXBean();
      double systemloadaverage = getProcessCpuLoad();
      MemoryUsage heap = membean.getHeapMemoryUsage();
      double heapusage = heap.getUsed() / (double) heap.getMax() * 100.0;
      MemoryUsage nonheap = membean.getNonHeapMemoryUsage();
      double nonheapusage = nonheap.getUsed() / (double) nonheap.getMax() * 100.0;
      String[] cpuheap = {systemloadaverage + DataSamudayaConstants.EMPTY, heapusage + DataSamudayaConstants.EMPTY,
          nonheapusage + DataSamudayaConstants.EMPTY};

      writer.write(new ObjectMapper().writeValueAsString(cpuheap));
      writer.flush();
    } catch (Exception ex) {
      log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
    }
  }

  public static double getProcessCpuLoad() throws Exception {

    MBeanServer mbs = ManagementFactory.getPlatformMBeanServer();
    ObjectName name = ObjectName.getInstance("java.lang:type=OperatingSystem");
    AttributeList list = mbs.getAttributes(name, new String[] {"ProcessCpuLoad"});
    if (list.isEmpty()) {
      return Double.NaN;
    }
    Attribute att = (Attribute) list.get(0);
    Double value = (Double) att.getValue();
    if (value == -1.0) {
      return Double.NaN;
    }
    return (int) (value * 1000) / 10.0;
  }
}
