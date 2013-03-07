package org.usergrid.vx.server.operations;

import java.util.List;

import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ListKeyspacesHandler implements Handler<Message<JsonObject>> {

  private Logger log = LoggerFactory.getLogger(ListKeyspacesHandler.class);

  @Override
  public void handle(Message<JsonObject> event) {
    log.debug("in ListKeyspaceHandler#handle");

    Integer id = event.body.getInteger("id");

    JsonObject response = new JsonObject().putArray(id.toString(), new JsonArray((List)Schema.instance.getNonSystemTables()));

    event.reply(response);
  }
}
