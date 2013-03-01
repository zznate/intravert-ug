package org.usergrid.vx.handler;

import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

/**
 * @author zznate
 */
public abstract class IntraHandlerBase implements IntraHandler {



  protected final Vertx vertx;

  public IntraHandlerBase(Vertx vertx) {
    this.vertx = vertx;
    registerRequestHandler();
  }

  @Override
 	public void handle(final HttpServerRequest request) {
 		request.bodyHandler( new Handler<Buffer>() {
 			public void handle(Buffer buffer) {
 			     handleRequestAsync(request, buffer);
 			}
 		});
 	}




}
