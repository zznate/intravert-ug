package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.processor.Processor;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;

public class ProcessorHandler implements Handler<Message<JsonArray>>{

  private Processor processor;
  
  public ProcessorHandler(Processor p){
    processor = p;
  }
  
  @Override
  public void handle(Message<JsonArray> arg0) {
    // TODO Auto-generated method stub
    
  }

}
