package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.CounterMutation;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.IntraService;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.util.ArrayList;
import java.util.List;

/**
 * Handler class for counter writes
 *
 * @author zznate
 */
public class CounterHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    RowMutation rm = new RowMutation(HandlerUtils.determineKs(params, state, null),
            IntraService.byteBufferForObject(params.getString("rowkey")));

    rm.addCounter(new QueryPath(
            HandlerUtils.determineCf(params, state, null ),
            null,
            IntraService.byteBufferForObject(params.getString("name"))),
            Long.parseLong(params.toMap().get("value").toString()));
    List<IMutation> mutations = new ArrayList(1);
    // TODO fix hard-coded consistency
    mutations.add(new CounterMutation(rm, ConsistencyLevel.ONE));
    HandlerUtils.write(mutations, event, id);
  }
}
