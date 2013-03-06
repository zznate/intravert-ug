package org.usergrid.vx.server.operations;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.List;

/**
 * @author zznate
 */
public class ListColumnFamilyHandler  implements Handler<Message<JsonObject>> {

  private Logger log = LoggerFactory.getLogger(ListColumnFamilyHandler.class);

  @Override
  public void handle(Message<JsonObject> event) {
    log.debug("in ListColumnFamilyHandler#handle");

    Integer id = event.body.getInteger("id");

    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    KSMetaData ks = Schema.instance.getKSMetaData(HandlerUtils.determineKs(params, state, null));
    JsonObject response = new JsonObject().putArray(id.toString(), new JsonArray(ks.cfMetaData().keySet().toArray()));

    event.reply(response);
  }
}
