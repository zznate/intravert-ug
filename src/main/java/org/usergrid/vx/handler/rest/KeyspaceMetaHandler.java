package org.usergrid.vx.handler.rest;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

/**
 * @author zznate
 */
public class KeyspaceMetaHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> jsonObjectMessage) {

  }
}
