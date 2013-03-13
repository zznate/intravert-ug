package org.usergrid.vx.server.operations;

import groovy.lang.GroovyClassLoader;

import org.usergrid.vx.experimental.ServiceProcessor;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class CreateServiceProcessHandler implements Handler<Message<JsonObject>>{

  private EventBus eb;
  
  public CreateServiceProcessHandler(EventBus eb){
    this.eb=eb;
  }
  
  @Override
  public void handle(Message<JsonObject> event) {
    System.out.println("creating");
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    String name = params.getString("name");
    String lang = params.getString("spec");
    String scriptSource = params.getString("value");
    
    GroovyClassLoader gc = new GroovyClassLoader(this.getClass().getClassLoader());
    Class c = gc.parseClass( scriptSource );
    ServiceProcessor sp = null;
    try {
      sp = (ServiceProcessor) c.newInstance();
      System.out.println( "Is sp == null" + (sp == null));
      System.out.println("registering processor named"+ " sps." + name);
      eb.registerHandler("sps." + name.toLowerCase(), new ServiceProcessorHandler(sp,eb));
    } catch (InstantiationException | IllegalAccessException e) {
      event.reply(new JsonObject()
      .putString(id.toString(), e.getClass().getName())
      .putString("exception", e.getMessage())
      .putString("exceptionId", id.toString()));
    }
    event.reply(new JsonObject().putString(id.toString(), "WOK"));
  }
}

