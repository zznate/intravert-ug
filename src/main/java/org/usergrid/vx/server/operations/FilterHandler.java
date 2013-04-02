package org.usergrid.vx.server.operations;

import org.mozilla.javascript.Context;
import org.usergrid.vx.experimental.filter.Filter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Map;

public class FilterHandler implements Handler<Message<JsonArray>> {

  private Filter filter;

  public FilterHandler(Filter filter) {
    this.filter = filter;
  }

  @Override
  public void handle(Message<JsonArray> event) {
    JsonArray filteredArray = new JsonArray();
    Context context = Context.enter();
    for (Object obj : event.body) {
      JsonObject jsonObject = (JsonObject) obj;
      Map filtered = filter.filter(jsonObject.toMap());
      if (filtered != null) {
        filteredArray.add(new JsonObject(filtered));
      }
    }
    event.reply(filteredArray);
  }

}
