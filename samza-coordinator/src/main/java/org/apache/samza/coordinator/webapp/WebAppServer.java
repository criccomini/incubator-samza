/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.samza.coordinator.webapp;

import java.util.Map;

import javax.servlet.Servlet;

import org.eclipse.jetty.server.Connector;
import org.eclipse.jetty.server.Server;
import org.eclipse.jetty.servlet.DefaultServlet;
import org.eclipse.jetty.servlet.ServletContextHandler;
import org.eclipse.jetty.servlet.ServletHolder;

public class WebAppServer {
  private static final ServletHolder DEFAULT_HOLDER = new ServletHolder(DefaultServlet.class);

  static {
    DEFAULT_HOLDER.setName("default");
  }

  private final String rootPath;
  private final Server server;
  private final ServletContextHandler context;
  private final Map<String, Servlet> servlets;
  private final String resourceBasePath;

  public WebAppServer(Map<String, Servlet> servlets) {
    this(servlets, 0);
  }

  public WebAppServer(Map<String, Servlet> servlets, int port) {
    this(servlets, "/", null, port);
  }

  public WebAppServer(Map<String, Servlet> servlets, String rootPath) {
    this(servlets, rootPath, null);
  }

  public WebAppServer(Map<String, Servlet> servlets, String rootPath, String resourceBasePath) {
    this(servlets, rootPath, resourceBasePath, 0);
  }

  public WebAppServer(Map<String, Servlet> servlets, String rootPath, String resourceBasePath, int port) {
    this.servlets = servlets;
    this.rootPath = rootPath;
    this.resourceBasePath = resourceBasePath;
    this.server = new Server(port);
    this.context = new ServletContextHandler(ServletContextHandler.SESSIONS);
  }

  public int start() throws Exception {
    context.setContextPath(rootPath);
    server.setHandler(context);
    context.addServlet(DEFAULT_HOLDER, "/css/*");
    context.addServlet(DEFAULT_HOLDER, "/js/*");

    if (resourceBasePath != null) {
      context.setResourceBase(getClass().getClassLoader().getResource(resourceBasePath).toExternalForm());
    }

    for (Map.Entry<String, Servlet> servletEntry : servlets.entrySet()) {
      String path = servletEntry.getKey();
      Servlet servlet = servletEntry.getValue();

      context.addServlet(new ServletHolder(servlet), path);
    }

    server.start();

    return ((Connector) server.getConnectors()[0]).getLocalPort();
  }

  public void stop() throws Exception {
    context.stop();
    server.stop();
  }
}
