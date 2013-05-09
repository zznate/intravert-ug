package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.ColumnFamily;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ReadHandler {

  private JsonObject params;

  private JsonObject state;

  private Message<JsonObject> event;

  private EventBus eb;

  public ReadHandler(Message<JsonObject> event, EventBus eb) {
    this.event = event;
    params = event.body.getObject("op");
    state = event.body.getObject("state");
    this.eb = eb;
  }

  public void handleRead(ColumnFamily cf) {
    final Integer id = event.body.getInteger("id");
    JsonArray array;

    if (cf == null) {
      event.reply(new JsonObject().putArray(id.toString(), new JsonArray()));
    } else {
      String filter = state.getString("currentFilter");
      if (filter == null) {
        array = HandlerUtils.instance.readCf(cf, state, params);
        JsonObject response = new JsonObject();
        response.putArray(id.toString(), array);
        event.reply(response);
      } else {
        HandlerUtils.instance.readCf(cf, state, eb, new Handler<Message<JsonArray>>() {
          @Override
          public void handle(Message<JsonArray> filterEvent) {
            event.reply(new JsonObject().putArray(id.toString(), filterEvent.body));
          }
        });
      }
    }
  }

}
