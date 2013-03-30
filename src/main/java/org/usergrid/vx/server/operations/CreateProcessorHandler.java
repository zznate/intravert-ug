package org.usergrid.vx.server.operations;

import groovy.lang.GroovyClassLoader;

import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.IntraState;
import org.usergrid.vx.experimental.processor.FactoryProvider;
import org.usergrid.vx.experimental.processor.Processor;
import org.usergrid.vx.experimental.processor.ProcessorFactory;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/*
 *   return new IntraOp(IntraOp.Type.CREATEPROCESSOR)
            .set(NAME,name)
            .set(SPEC, spec)
            .set(VALUE, value);
 */
public class CreateProcessorHandler implements Handler<Message<JsonObject>>{
  
  private EventBus eb;

  public CreateProcessorHandler(EventBus eb) {
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
      ProcessorFactory processorFactory = factoryProvider.getFilterFactory(lang);
      Processor processor = processorFactory.createProcessor(scriptSource);
      eb.registerHandler("processors." + name, new ProcessorHandler(processor));
      event.reply(new JsonObject().putString(id.toString(), "OK"));
    } catch (IllegalArgumentException e) {
      event.reply(new JsonObject()
        .putString(id.toString(), e.getClass().getName())
        .putString("exception", e.getMessage())
        .putNumber("exceptionId", id));
    } 
    
  }

}
