package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.filter.FactoryProvider;
import org.usergrid.vx.experimental.filter.Filter;
import org.usergrid.vx.experimental.filter.FilterFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class CreateFilterHandler implements Handler<Message<JsonObject>> {

  private EventBus eb;

  public CreateFilterHandler(EventBus eb) {
    this.eb = eb;
  }

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body().getInteger("id");
    JsonObject params = event.body().getObject("op");

    String name = params.getString("name");
    String lang = params.getString("spec");
    String scriptSource = params.getString("value");

    FactoryProvider factoryProvider = new FactoryProvider();
    try {
      FilterFactory filterFactory = factoryProvider.getFilterFactory(lang);
      Filter filter = filterFactory.createFilter(scriptSource);
      eb.registerHandler("filters." + name, new FilterHandler(filter));
      HandlerUtils.instance.putFilter(name, filterFactory.createFilter(scriptSource));

      event.reply(new JsonObject().putString(id.toString(), "OK"));
    } catch (IllegalArgumentException e) {
      event.reply(new JsonObject()
        .putString(id.toString(), e.getClass().getName())
        .putString("exception", e.getMessage())
        .putString("exceptionId", id.toString()));
    }
  }

}
