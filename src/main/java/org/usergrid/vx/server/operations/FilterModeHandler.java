package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.IntraState;
import org.usergrid.vx.experimental.filter.Filter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class FilterModeHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    String filterName = params.getString("name");
    Boolean enabled = params.getBoolean("on");

    Filter filter = IntraState.filters.get(filterName);
    if (enabled) {
      if (filter == null) {
        event.reply(new JsonObject()
            .putString(id.toString(), "ERROR")
            .putString("exception", "filter " + filterName + " not found"));
      } else {
        state.putString("currentFilter", filterName);
      }
    } else {
      state.removeField("currentFilter");
    }
    event.reply(new JsonObject()
      .putString(id.toString(), "OK")
      .putObject("state", state));
  }
}
