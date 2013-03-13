package org.usergrid.vx.handler.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

/**
 * @author zznate
 * @author boneill42
 */
public class ColumnFamilyRestHandler extends IntraHandlerRest {
  private Logger log = LoggerFactory.getLogger(ColumnFamilyRestHandler.class);

  public ColumnFamilyRestHandler(Vertx vertx) {
    super(vertx);
  }

  public void handlePost(final HttpServerRequest request, Buffer buffer, IntraReq req) {
    req.add(Operations.setKeyspaceOp(request.params().get("ks")));    
    req.add(Operations.createCfOp(request.params().get("cf")));
  }
}
