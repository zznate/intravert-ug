package org.usergrid.vx.server.operations;

import java.util.List;
import java.util.Map;

import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class MultiProcessorHandler implements Handler<Message<JsonObject>> {
  
  MultiProcessor p;
  
  public MultiProcessorHandler(MultiProcessor p){
    this.p=p;
  }
  
  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    Map params = event.body.getObject("mpparams").toMap();
    Map mpres = event.body.getObject("mpres").toMap();
    List<Map> results = p.multiProcess(mpres, params);
    JsonArray ja = new JsonArray();
    for (Map result: results){
      ja.addObject( new JsonObject(result));
    }
    event.reply(new JsonObject().putArray(id.toString(), ja));
  }

}