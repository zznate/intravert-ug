package org.usergrid.vx.server.operations;

import java.util.Arrays;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.usergrid.vx.experimental.IntraService;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class SetHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        Integer id = event.body.getInteger("id");
        JsonObject params = event.body.getObject("op");
        JsonObject state = event.body.getObject("state");

        RowMutation rm = null;
        String ks = null;
        String cf = null;

        if (params.getString("keyspace") != null) {
            ks = params.getString("keyspace");
        } else {
            ks = state.getString("currentKeyspace");
        }

        if (params.getString("columnfamily") != null) {
            cf = params.getString("columnfamily");
        } else {
            cf = state.getString("currentColumnFamily");
        }

        rm = new RowMutation(ks, IntraService.byteBufferForObject(params.getString("rowkey")));
        QueryPath qp = new QueryPath(cf, null, IntraService.byteBufferForObject(params.getString("name")));
        Object val = params.toMap().get("value");

        Integer ttl = params.getInteger("ttl");
        if (ttl == null) {
            // TODO add autoTimestamp and nanotime to the state object sent in the event bus message
            rm.add(qp, IntraService.byteBufferForObject(IntraService.resolveObject(val, null, null, null, id)),
                System.nanoTime());
        } else {
            rm.add(qp, IntraService.byteBufferForObject(IntraService.resolveObject(val, null, null, null, id)),
                System.nanoTime(), ttl);
        }

        try {
            // We don't want to hard code the consistency level but letting it slide for
            // since it is also hard coded in IntraState
            StorageProxy.mutate(Arrays.asList(rm), ConsistencyLevel.ONE);

            event.reply(new JsonObject().putString(id.toString(), "OK"));
        } catch (WriteTimeoutException | UnavailableException | OverloadedException e) {
            event.reply(new JsonObject()
                .putString("exception", e.getMessage())
                .putString("exceptionId", id.toString()));
        }
    }
}
