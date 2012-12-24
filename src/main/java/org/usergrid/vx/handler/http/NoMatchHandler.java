package org.usergrid.vx.handler.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

public class NoMatchHandler implements Handler<HttpServerRequest>{
	
	private final Logger logger = LoggerFactory.getLogger(IntravertCassandraServer.class);

  @Override
  public void handle(HttpServerRequest request) {
    logger.error("no matching endpoint for "+ request.uri);
    request.response.end("No Matching Endpoint.");
  }

}
