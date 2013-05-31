package org.usergrid.vx.server.operations;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.ColumnFamily;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ReadHandler {

  private JsonObject params;
  private JsonObject state;
  private Message<JsonObject> event;
  private EventBus eb;
  private ByteBuffer rowKey;

  public ReadHandler(Message<JsonObject> event, EventBus eb) {
    this.event = event;
    params = event.body().getObject("op");
    state = event.body().getObject("state");
    this.eb = eb;
  }

  public void handleRead(ColumnFamily cf) {
    final Integer id = event.body().getInteger("id");
    JsonArray array;
    final JsonObject resultMode = HandlerUtils.instance.getResultMode(state);
    if (cf == null) {
      event.reply(new JsonObject().putArray(id.toString(), new JsonArray()));
    } else {
      String filter = state.getString("currentFilter");
      if (filter == null) {
        array = HandlerUtils.instance.internalCfRead(cf, state);
        JsonObject response = new JsonObject();
        if (resultMode == null){
          
          response.putArray(id.toString(), array);
          event.reply(response);
        } else {
          //System.out.println("This will fail fix me ");
          //eb.send("operations.batchset", array);
        }
        
      } else {
        HandlerUtils.instance.readCf(cf, state, eb, new Handler<Message<JsonArray>>() {
          @Override
          public void handle(final Message<JsonArray> filterEvent) {
            
            if (resultMode == null){
              event.reply(new JsonObject().putArray(id.toString(), filterEvent.body()));
            } else {
              JsonObject obj = new JsonObject();
              obj.putObject(Operations.OP, new JsonObject().putArray("rows", filterEvent.body()) );
              obj.putObject(Operations.STATE, state);
              obj.putNumber(Operations.ID, 999);
              eb.send("operations.batchset", obj, new Handler<Message<JsonObject>>(){
                @Override
                public void handle(Message<JsonObject> arg0) {
                  event.reply(new JsonObject().putArray(id.toString(), filterEvent.body()));
                }
              });
            }
              
          }
        });
      }
    }
  }

}
