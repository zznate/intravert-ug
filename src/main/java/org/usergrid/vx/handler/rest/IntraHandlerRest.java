package org.usergrid.vx.handler.rest;

import org.usergrid.vx.handler.IntraHandlerBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;

/**
 * @author zznate
 */
public class IntraHandlerRest extends IntraHandlerBase {

  public IntraHandlerRest(Vertx vertx) {
    super(vertx);
  }

  public void handleRequestAsync(final HttpServerRequest req, Buffer buffer) {
    Map<String,String> reqParams = req.params();
    // TODO  extract query, etc? or delegate lower. Probably lower given current delegation in virgil

    // TODO extract keyspace (null is ok as this triggers getKeyspaces()

    // TODO extract consistencyLevel header if available

    // dispatch to handler

  }

  public void registerRequestHandler() {
    vertx.eventBus().registerHandler("data.keyspaceMeta", new KeyspaceMetaHandler());

  }


}
