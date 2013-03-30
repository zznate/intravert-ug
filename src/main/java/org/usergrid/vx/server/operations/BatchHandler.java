package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class BatchHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    JsonArray array = params.getArray("rows");
    List<IMutation> mutations = new ArrayList<IMutation>();
    for (int i =0;i<array.size();i++){
      JsonObject row = (JsonObject) array.get(i);
      RowMutation rm = new RowMutation(HandlerUtils.determineKs(params, state, row),
              HandlerUtils.byteBufferForObject(row.getField("rowkey")));
      QueryPath qp = new QueryPath(HandlerUtils.determineCf(params, state, row),
              null, HandlerUtils.byteBufferForObject(row.getField("name")));
      Object val = row.getField("value");
      Integer ttl = row.getInteger("ttl");
      if (ttl == null) {
          // TODO add autoTimestamp and nanotime to the state object sent in the event bus message
          rm.add(qp, HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(val)),
              System.nanoTime());
      } else {
          rm.add(qp, HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(val)),
              System.nanoTime(), ttl);
      }
      mutations.add(rm);
    }
    HandlerUtils.write(mutations, event, id);
  }

}
