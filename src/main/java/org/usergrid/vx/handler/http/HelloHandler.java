package org.usergrid.vx.handler.http;

import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

public class HelloHandler implements Handler<HttpServerRequest>{

  @Override
  public void handle(HttpServerRequest request) {
    System.out.println("Request handled");
    request.response.end("ok");
  }
	
}
