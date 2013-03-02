package org.usergrid.vx.server.operations;

import java.util.Set;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class ComponentSelectHandler implements Handler<Message<JsonObject>>{
  
  /*
  Set<String> parts = (Set<String>) op.getOp().get("components");
  state.components = parts;
  */
  
  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    //state.putBoolean("autotimestamp", params.getBoolean("autotimestamp"));
    System.out.println ("components is "+params.getArray("components"));
    state.putArray("components", params.getArray("components"));
    event.reply(new JsonObject()
        .putString(id.toString(), "OK")
        .putObject("state", state)
    );
    
  }

}
