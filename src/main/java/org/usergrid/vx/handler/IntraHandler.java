package org.usergrid.vx.handler;

import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

/**
 * Base Handler interface for Intravert HTTP handlers
 *
 * @author zznate
 */
public interface IntraHandler extends Handler<HttpServerRequest> {

  /**
   * Handle the provided request asynchronously
   * @param req
   * @param buffer
   */
  void handleRequestAsync(HttpServerRequest req, Buffer buffer);

  void registerRequestHandler();

}
