package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.List;

import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class SetHandler extends AbstractIntravertHandler{

  @Override
  public void handleUser(Message<JsonObject> event) {
    Integer id = event.body.getInteger(Operations.ID);
    JsonObject params = event.body.getObject(Operations.OP);
    JsonObject state = event.body.getObject(Operations.STATE);
    RowMutation rm = new RowMutation(HandlerUtils.instance.determineKs(params, state, null),
            HandlerUtils.instance.byteBufferForObject(params.getField(Operations.ROWKEY)));
    QueryPath qp = new QueryPath(HandlerUtils.instance.determineCf(params, state, null), null,
            HandlerUtils.instance.byteBufferForObject(params.getField(Operations.NAME)));
    Object val = params.getField(Operations.VALUE);
    Integer ttl = params.getInteger(Operations.TTL);
    if (ttl == null) {
      // TODO add autoTimestamp and nanotime to the state object sent in the event bus message
      rm.add(qp, HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(val)), System.nanoTime());
    } else {
      rm.add(qp, HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(val)), System.nanoTime(), ttl);
    }
    List<IMutation> mutations = new ArrayList<IMutation>();
    mutations.add(rm);
    HandlerUtils.write(mutations, event, id);
  }
}
