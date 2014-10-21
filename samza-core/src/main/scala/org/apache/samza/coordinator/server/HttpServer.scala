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

package org.apache.samza.coordinator.server;

import java.net.InetAddress
import java.net.URI
import java.net.UnknownHostException
import javax.servlet.Servlet
import org.apache.samza.SamzaException
import org.eclipse.jetty.server.Connector
import org.eclipse.jetty.server.Server
import org.eclipse.jetty.servlet.DefaultServlet
import org.eclipse.jetty.servlet.ServletContextHandler
import org.eclipse.jetty.servlet.ServletHolder;
import java.net.URL

class HttpServer(
  rootPath: String = "/",
  port: Int = 0,
  resourceBasePath: String = null,
  defaultHolder: ServletHolder = new ServletHolder(classOf[DefaultServlet])) {

  var servlets = Map[String, Servlet]()
  val server = new Server(port)
  val context = new ServletContextHandler(ServletContextHandler.SESSIONS)

  defaultHolder.setName("default")

  def addServlet(path: String, servlet: Servlet) {
    servlets += path -> servlet
  }

  def start {
    context.setContextPath(rootPath)
    server.setHandler(context)
    context.addServlet(defaultHolder, "/css/*")
    context.addServlet(defaultHolder, "/js/*")

    // TODO This is where you'd add Hadoop's Kerberos security filters.
    // context.addFilter(classOf[YourApplicationEndpointFilter], "/*", 0)

    if (resourceBasePath != null) {
      context.setResourceBase(getClass.getClassLoader.getResource(resourceBasePath).toExternalForm())
    }

    servlets.foreach {
      case (path, servlet) =>
        context.addServlet(new ServletHolder(servlet), path);
    }

    server.start()
  }

  def stop {
    context.stop()
    server.stop()
  }

  def getUrl = {
    val runningPort = server.getConnectors()(0).asInstanceOf[Connector].getLocalPort()
    new URL("http://" + InetAddress.getLocalHost().getHostName() + ":" + runningPort + rootPath)
  }
}
