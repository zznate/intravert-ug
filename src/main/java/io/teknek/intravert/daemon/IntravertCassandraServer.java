package io.teknek.intravert.daemon;
import io.teknek.intravert.service.DefaultIntravertService;
import io.teknek.intravert.service.IntravertService;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

import javax.servlet.ServletException;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletRequest;

import org.apache.cassandra.service.CassandraDaemon.Server;
import org.codehaus.jackson.map.ObjectMapper;
import org.mortbay.jetty.Handler;
import org.mortbay.jetty.HttpConnection;
import org.mortbay.jetty.Request;
import org.mortbay.jetty.handler.AbstractHandler;

public class IntravertCassandraServer implements Server {

  private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
  private org.mortbay.jetty.Server server;
  public static final int port = 7654;
  private IntravertService intraService;
  private static ObjectMapper MAPPER = new ObjectMapper();

  @Override
  public boolean isRunning() {
    return RUNNING.get();
  }

  @Override
  public void start() {
    intraService = new DefaultIntravertService();
    server = new org.mortbay.jetty.Server(port);
    server.setHandler(getHandler());
    try {
      server.start();
      RUNNING.set(true);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void stop() {
    try {
      server.stop();
      RUNNING.set(false);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }    
  }

  private Handler getHandler(){
    final IntravertService copy = this.intraService;
    AbstractHandler handler = new AbstractHandler() {
      public void handle(String target, HttpServletRequest request, HttpServletResponse response,
              int dispatch) throws IOException, ServletException {
        Request baseRequest = request instanceof Request ? (Request) request : HttpConnection
                .getCurrentConnection().getRequest();
        String url = baseRequest.getRequestURI();
        io.teknek.intravert.model.Request requestFromBody = MAPPER.readValue(baseRequest.getInputStream(), io.teknek.intravert.model.Request.class);
        response.setStatus(HttpServletResponse.SC_OK);        
        baseRequest.setHandled(true);
        MAPPER.writeValue(response.getOutputStream(), copy.doRequest(requestFromBody));
      }
    };
    return handler;

  }
}
