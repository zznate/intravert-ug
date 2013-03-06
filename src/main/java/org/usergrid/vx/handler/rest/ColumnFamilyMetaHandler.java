package org.usergrid.vx.handler.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * @author zznate
 */
public class ColumnFamilyMetaHandler extends IntraHandlerRest {

  private Logger log = LoggerFactory.getLogger(ColumnFamilyMetaHandler.class);

  public ColumnFamilyMetaHandler(Vertx vertx) {
    super(vertx);
  }

  @Override
  public void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    log.debug("In ColumnFamilyMetaHandler#handleRequestAsync");
    IntraReq req = new IntraReq();
    req.add(Operations.listColumnFamilyOp(request.params().get("ks")));
    vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        request.response.end(event.body.toString());
      }
    });
  }
}
