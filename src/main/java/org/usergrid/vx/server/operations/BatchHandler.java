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

public class BatchHandler extends AbstractIntravertHandler {

  @Override
  public void handleUser(Message<JsonObject> event) {
    System.out.println("I gots da batch "+event.body());
    Integer id = event.body().getInteger("id");
    JsonObject params = event.body().getObject("op");
    JsonObject state = event.body().getObject("state");
    JsonArray array = params.getArray("rows");
    List<IMutation> mutations = new ArrayList<IMutation>();
    for (int i =0;i<array.size();i++){
      JsonObject row = (JsonObject) array.get(i);
      RowMutation rm = new RowMutation(HandlerUtils.instance.determineKs(params, state, row),
              HandlerUtils.instance.byteBufferForObject(row.getField("rowkey")));
      QueryPath qp = new QueryPath(HandlerUtils.instance.determineCf(params, state, row),
              null, HandlerUtils.instance.byteBufferForObject(row.getField("name")));
      Object val = row.getField("value");
      Integer ttl = row.getInteger("ttl");
      if (ttl == null) {
          rm.add(qp, HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(val)),
                  HandlerUtils.instance.determineTimestamp(params, state, row));
      } else {
          rm.add(qp, HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(val)),
                  HandlerUtils.instance.determineTimestamp(params, state, row), ttl);
      }
      mutations.add(rm);
    }
    HandlerUtils.instance.write(mutations, event, id, state);
  }

}
