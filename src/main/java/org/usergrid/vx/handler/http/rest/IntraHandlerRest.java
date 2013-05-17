package org.usergrid.vx.handler.http.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.handler.IntraHandlerBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * @author zznate
 * @author boneill42
 */
public abstract class IntraHandlerRest extends IntraHandlerBase {
  private final Logger log = LoggerFactory.getLogger(IntraHandlerRest.class);
  
  public IntraHandlerRest(Vertx vertx) {
    super(vertx);
  }

  @Override
  public void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    IntraReq req = new IntraReq();
    if (request.method.equals("GET")) {
      handleGet(request, buffer, req);
    } else if (request.method.equals("POST")) {
      handlePost(request, buffer, req);
    } else if (request.method.equals("DELETE")) {
      handleDelete(request, buffer, req);
    } else if (request.method.equals("PUT")) {
      handlePut(request, buffer, req);
    }    
    delegateAndReply(request, req);
  }
  
  protected void delegateAndReply(final HttpServerRequest request, IntraReq req) {
    vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        request.response.end(event.body.toString());
      }
    });
  }
  
  public void handleGet(final HttpServerRequest request, Buffer buffer, IntraReq req) {
    throw new RuntimeException("GET not supported by this handler [" + this.getClass().getSimpleName() + "]");
  }

  public void handlePost(final HttpServerRequest request, Buffer buffer, IntraReq req) {
    throw new RuntimeException("POST not supported by this handler [" + this.getClass().getSimpleName() + "]");
  }

  public void handleDelete(final HttpServerRequest request, Buffer buffer, IntraReq req) {
    throw new RuntimeException("DELETE not supported by this handler [" + this.getClass().getSimpleName() + "]");
  }

  public void handlePut(final HttpServerRequest request, Buffer buffer, IntraReq req) {
    throw new RuntimeException("PUT not supported by this handler [" + this.getClass().getSimpleName() + "]");
  }
}
