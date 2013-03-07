package org.usergrid.vx.handler.rest;

import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.handler.IntraHandlerBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * @author zznate
 * @author boneill42
 */
public abstract class IntraHandlerRest extends IntraHandlerBase {
  private final Logger logger = LoggerFactory.getLogger(IntraHandlerRest.class);
  
  public IntraHandlerRest(Vertx vertx) {
    super(vertx);
  }

  protected void delegateAndReply(final HttpServerRequest request, IntraReq req) {
    vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        request.response.end(event.body.toString());
      }
    });
  }
}
