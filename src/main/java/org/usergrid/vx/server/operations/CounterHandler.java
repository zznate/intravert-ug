package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Handler class for counter writes
 *
 */
public class CounterHandler extends AbstractIntravertHandler {

  @Override
  public void handleUser(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    RowMutation rm = new RowMutation(HandlerUtils.instance.determineKs(params, state, null),
            HandlerUtils.instance.byteBufferForObject(params.getString("rowkey")));
    rm.addCounter(new QueryPath(
            HandlerUtils.instance.determineCf(params, state, null ),
            null,
            HandlerUtils.instance.byteBufferForObject(params.getString("name"))),
            Long.parseLong(params.toMap().get("value").toString()));
    List<IMutation> mutations = new ArrayList<IMutation>(1);
    mutations.add(new CounterMutation(rm, HandlerUtils.instance.determineConsistencyLevel(state)));
    HandlerUtils.instance.write(mutations, event, id, state);
  }
}