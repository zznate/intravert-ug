package org.usergrid.vx.handler.rest;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

/**
 * REST Handler for listing non-system keyspaces via
 * {@link org.apache.cassandra.config.Schema#getNonSystemTables()}
 *
 * @author zznate
 */
public class KeyspaceMetaHandler extends IntraHandlerRest {

  private Logger log = LoggerFactory.getLogger(KeyspaceMetaHandler.class);

  public KeyspaceMetaHandler(Vertx vertx) {
    super(vertx);
  }

  @Override
  public void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    log.debug("In KeyspaceMetaHandler#handleRequestAsync");
    IntraReq req = new IntraReq();
    req.add(Operations.listKeyspacesOp());
    delegateAndReply(request, req);
  }

}
