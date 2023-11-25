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

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.io.FilenameUtils;
import org.apache.log4j.Logger;

/**
 * Web Resources servlet to fetch the file for example (HTML,js) etc
 * and sends to the browser.
 * @author Arun 
 * 
 */
public class WebResourcesServlet extends HttpServlet {

  private static final long serialVersionUID = 8713220540678338208L;
  private static Logger log = Logger.getLogger(WebResourcesServlet.class);

  protected void doGet(HttpServletRequest request, HttpServletResponse response)
      throws IOException {

    response.setStatus(HttpServletResponse.SC_OK);
    String filename = request.getPathInfo();
    if (filename.endsWith(DataSamudayaConstants.JAVASCRIPT)) {
      response.setContentType(DataSamudayaConstants.TEXTJAVASCRIPT);
    } else if (filename.endsWith(DataSamudayaConstants.CSS)) {
      response.setContentType(DataSamudayaConstants.TEXTCSS);
    }
    File file = new File(DataSamudayaConstants.PREV_FOLDER + DataSamudayaConstants.FORWARD_SLASH
        + DataSamudayaConstants.WEB_FOLDER + filename);
    try (FileInputStream fis = new FileInputStream(file);
        ServletOutputStream sos = response.getOutputStream();) {

      byte[] buffer = new byte[4096];
      int numread;
      while ((numread = fis.read(buffer, 0, 4096)) != -1) {
        sos.write(buffer, 0, numread);
        sos.flush();
      }
    } catch (Exception ex) {
      log.debug("TaskScheduler Web servlet error, See cause below \n", ex);
    }
  }
}
