package org.usergrid.vx.server.operations;


import org.usergrid.vx.experimental.multiprocessor.FactoryProvider;
import org.usergrid.vx.experimental.multiprocessor.MultiProcessor;
import org.usergrid.vx.experimental.multiprocessor.MultiProcessorFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class CreateMultiProcessHandler implements Handler<Message<JsonObject>>{
  
  private EventBus eb;

  public CreateMultiProcessHandler(EventBus eb) {
    this.eb = eb;
  }

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    String name = params.getString("name");
    String lang = params.getString("spec");
    String scriptSource = params.getString("value");

    FactoryProvider factoryProvider = new FactoryProvider();
    try {
      MultiProcessorFactory processorFactory = factoryProvider.getFilterFactory(lang);
      MultiProcessor processor = processorFactory.createMultiProcessor(scriptSource);
      eb.registerHandler("multiprocessors." + name, new MultiProcessorHandler(processor));
      System.out.println("created multprocessor "+name);
      event.reply(new JsonObject().putString(id.toString(), "OK"));
    } catch (IllegalArgumentException e) {
      event.reply(new JsonObject()
        .putString(id.toString(), e.getClass().getName())
        .putString("exception", e.getMessage())
        .putNumber("exceptionId", id));
    } 
    
  }
}
