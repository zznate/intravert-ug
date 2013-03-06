package org.usergrid.vx.server.operations;

import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.commons.lang.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

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

    String keyspace = HandlerUtils.determineKs(params, state, null);
    if (StringUtils.isBlank(keyspace)) {
      event.reply(HandlerUtils.buildError(id, "keyspace cannot be blank"));
      return;
    }
    KSMetaData ks = Schema.instance.getKSMetaData(keyspace);
    if ( ks == null ) {
      event.reply(HandlerUtils.buildError(id, String.format("Keyspace %s did not exist", keyspace)));
      return;
    }
    JsonObject response = new JsonObject().putArray(id.toString(), new JsonArray(ks.cfMetaData().keySet().toArray()));

    event.reply(response);
  }
}
