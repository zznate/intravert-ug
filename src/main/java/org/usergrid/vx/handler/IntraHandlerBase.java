package org.usergrid.vx.handler;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.usergrid.vx.handler.rest.RestOperationNotSupportedException;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

/**
 * Base class which uses {@link #handle(org.vertx.java.core.http.HttpServerRequest)}
 * for delegation to {@link IntraHandler#handleRequestAsync(org.vertx.java.core.http.HttpServerRequest, org.vertx.java.core.buffer.Buffer)}
 *
 * This gives sub classes access to the request and the working Buffer.
 *
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
    try {
      request.bodyHandler( new Handler<Buffer>() {
        public void handle(Buffer buffer) {
          handleRequestAsync(request, buffer);
        }
      });

    } catch (Exception e) {
      request.response.statusCode = BAD_REQUEST.getCode();
      request.response.end(ExceptionUtils.getFullStackTrace(e));
    }
  }




}
