package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.usergrid.vx.experimental.processor.Processor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ProcessorHandler implements Handler<Message<JsonObject>> {

  private Processor processor;
  
  public ProcessorHandler(Processor p){
    processor = p;
  }
  
  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body().getInteger("id");
    JsonArray input = event.body().getArray("input");
    List<Map> rows = new ArrayList<Map>();
    for (int i = 0 ; i < input.size() ; i++){
      rows.add( ((JsonObject) input.get(i)).toMap());
    }
    List<Map> results = processor.process(rows);
    JsonArray ja = new JsonArray();
    for (Map result: results){
      ja.addObject( new JsonObject(result));
    }
    event.reply(new JsonObject().putArray(id.toString(), ja));
    
  }

}
