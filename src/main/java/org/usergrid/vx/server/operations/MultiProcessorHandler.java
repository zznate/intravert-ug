package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.usergrid.vx.experimental.IntraOp;
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
  /*
   * JsonObject params = operation.getObject("op");
        JsonObject theParams = operation.getObject("params");
        operation.putObject("mpparams", theParams);
        operation.putObject("mpres", results.getObject("opsRes"));        
        System.out.println("sendingevent to"+ params.getString("name"));
        vertx.eventBus().send("multiprocessors." + params.getString("name"), operation, this);
   */

  /*
   *         IntraOp op = req.getE().get(i);
        String name = (String) op.getOp().get("name");
        Map params  = (Map) op.getOp().get("params");
        //Processor p = state.processors.get(processorName);
        MultiProcessor p = state.multiProcessors.get(name);

        List<Map> mpResults =  p.multiProcess(res.getOpsRes(), params);
        res.getOpsRes().put(i, mpResults);
      }
   */
  @Override
  public void handle(Message<JsonObject> event) {
    System.out.println(event);
    Integer id = event.body.getInteger("id");
    Map params = event.body.getObject("mpparams").toMap();
    System.out.println(params);
    Map mpres = event.body.getObject("mpres").toMap();
    System.out.println(mpres);
    List<Map> results = p.multiProcess(mpres, params);
    System.out.println(results);
    JsonArray ja = new JsonArray();
    for (Map result: results){
      ja.addObject( new JsonObject(result));
    }
    event.reply(new JsonObject().putArray(id.toString(), ja));
    
  }

}
