package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.filter.Filter;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class FilterModeHandler extends AbstractIntravertHandler {

  @Override
  public void handleUser(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    String filterName = params.getString("name");
    Boolean enabled = params.getBoolean("on");

    if (enabled) {
      Filter filter = HandlerUtils.instance.getFilter(filterName);
      if (filter == null) {
        event.reply(new JsonObject()
            .putString(id.toString(), "ERROR")
            .putString("exception", "filter " + filterName + " not found"));
      } else {
        HandlerUtils.instance.activateFilter(state, filterName);
      }
    } else {
      HandlerUtils.instance.deactivateFilter(state);
    }
    event.reply(new JsonObject()
      .putString(id.toString(), "OK")
      .putObject("state", state));
  }
}
