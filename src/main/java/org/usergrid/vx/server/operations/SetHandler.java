package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class SetHandler extends AbstractIntravertHandler{

  @Override
  public void handleUser(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    RowMutation rm = new RowMutation(HandlerUtils.determineKs(params, state, null),
            HandlerUtils.byteBufferForObject(params.getField("rowkey")));
    QueryPath qp = new QueryPath(HandlerUtils.determineCf(params, state, null), null,
            HandlerUtils.byteBufferForObject(params.getField("name")));

    Object val = params.getField("value");
    Integer ttl = params.getInteger("ttl");
    if (ttl == null) {
      // TODO add autoTimestamp and nanotime to the state object sent in the event bus message
      rm.add(qp, HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(val)), System.nanoTime());
    } else {
      rm.add(qp, HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(val)), System.nanoTime(), ttl);
    }
    List<IMutation> mutations = new ArrayList<IMutation>();
    mutations.add(rm);
    HandlerUtils.write(mutations, event, id);
  }
}
