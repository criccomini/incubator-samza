package org.apache.samza.coordinator;

import java.io.IOException;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.samza.config.Config;
import org.codehaus.jackson.map.ObjectMapper;

@SuppressWarnings("serial")
public class SamzaCoordinatorConfigServlet extends HttpServlet {
  private final static ObjectMapper JSON_MAPPER = new ObjectMapper();

  private final Config config;

  public SamzaCoordinatorConfigServlet(Config config) {
    this.config = config;
  }

  protected void doGet(HttpServletRequest request, HttpServletResponse response) throws ServletException, IOException {
    response.setContentType("application/json");
    response.setStatus(HttpServletResponse.SC_OK);
    response.getWriter().println(JSON_MAPPER.writeValueAsString(config));
  }
}
