package org.usergrid.vx.server.operations;

import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class AutotimestampHandler extends AbstractIntravertHandler {

  @Override
  public void handleUser(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    state.putBoolean("autotimestamp", params.getBoolean("autotimestamp"));
    event.reply(new JsonObject()
        .putString(id.toString(), "OK")
        .putObject("state", state)
    );
  }
  
}
