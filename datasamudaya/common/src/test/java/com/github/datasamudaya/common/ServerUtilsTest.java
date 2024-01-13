/*
 * Copyright 2021 the original author or authors. <p> Licensed under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with the License. You may obtain
 * a copy of the License at <p> https://www.apache.org/licenses/LICENSE-2.0 <p> Unless required by
 * applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See
 * the License for the specific language governing permissions and limitations under the License.
 */
package com.github.datasamudaya.common;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.URL;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.compress.utils.IOUtils;
import org.junit.Test;

import junit.framework.TestCase;

public class ServerUtilsTest extends TestCase {

  public static final String testhtml = "<html><p>test</p></html>";
  public static final String message =
      "Server requires Port and atleast one servlet and url to access";
  public static final String instanceservletmessage =
      "test which is of type java.lang.String must be instance of servlet javax.servlet.http.HttpServlet";
  public static final String instanceservletmessagewithnoproperpath =
      "Path must be Url path of servlet " + TestServlet.class.getName();
  public static final String integerportmessage = "Configuration port must be integer";

  @Test
  public void testServerInitWithoutServlet() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init(1000);
    } catch (Exception ex) {
      assertEquals(message, ex.getMessage());
    }
  }

  @Test
  public void testServerInitWithNoPort() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init();
    } catch (Exception ex) {
      assertEquals(message, ex.getMessage());
    }
  }

  @Test
  public void testServerInitWithImproperPortType() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init("1000", new TestServlet(), "/test");
    } catch (Exception ex) {
      assertEquals(integerportmessage, ex.getMessage());
    }
  }

  @Test
  public void testServerInitWithServletNoPath() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init(1000, new TestServlet());
    } catch (Exception ex) {
      assertEquals(message, ex.getMessage());
    }
  }

  @Test
  public void testServerInitWithNoServletWithPath() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init(1000, "test", "/test");
    } catch (Exception ex) {
      assertEquals(instanceservletmessage, ex.getMessage());
    }
  }

  @Test
  public void testServerInitWithServletWithNoProperPath() throws Exception {
    try {
      ServerUtils serverutils = new ServerUtils();
      serverutils.init(1000, new TestServlet(), new TestServlet());
    } catch (Exception ex) {
      assertEquals(instanceservletmessagewithnoproperpath, ex.getMessage());
    }
  }

  @Test
  public void testServerHttpServlet() throws Exception {
    ServerUtils serverutils = new ServerUtils();
    serverutils.init(1000, new TestServlet(), "/test/*");
    serverutils.start();
    URL url = new URL("http://localhost:1000/test");
    InputStream is = url.openStream();
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    IOUtils.copy(is, baos);
    assertEquals(testhtml, new String(baos.toByteArray()));
    serverutils.stop();
    assertEquals(true, serverutils.server.isStopped());
    is.close();
  }

  public static class TestServlet extends HttpServlet {

    /**
     * 
     */
    private static final long serialVersionUID = 3270221092251177195L;

    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      OutputStream os = resp.getOutputStream();
      os.write(testhtml.getBytes());
      os.flush();
      os.close();
    }

    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp)
        throws ServletException, IOException {
      doGet(req, resp);
    }

  }

}
