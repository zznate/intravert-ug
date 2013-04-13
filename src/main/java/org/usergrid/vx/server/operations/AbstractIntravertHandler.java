package org.usergrid.vx.server.operations;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public abstract class AbstractIntravertHandler implements Handler<Message<JsonObject>>{

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = null;
    try {
      id = event.body.getInteger("id");
      handleUser(event);
    } catch (Exception ex){
      String message = ex.getMessage() ==null ? "Exception "+id : ex.getMessage() ;
      event.reply(new JsonObject().putString("exceptionId", id+"")
              .putString("exception", message));
    }
  }
  
  public abstract void handleUser(Message<JsonObject> event);

}
