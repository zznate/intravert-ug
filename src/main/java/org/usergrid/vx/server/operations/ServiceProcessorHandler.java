package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.ServiceProcessor;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class ServiceProcessorHandler implements Handler<Message<JsonObject>> {

  ServiceProcessor sp;
  EventBus eb;
  
  public ServiceProcessorHandler(ServiceProcessor sp, EventBus eb){
    this.sp=sp;
    this.eb=eb;
  }
  
  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = null;
    String res = "OK";
    try {
      id = event.body.getInteger("id");
      sp.process(event.body, event.body.getObject("state"), new JsonObject(), eb);
    } catch (Exception ex){
      res="FAILED";
    }
    event.reply(new JsonObject().putString(id.toString(), res));
  }

}
