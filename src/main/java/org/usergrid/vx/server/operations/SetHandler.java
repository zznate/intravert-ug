package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
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
        System.out.println(params);
        RowMutation rm = new RowMutation(HandlerUtils.determineKs(params,state),
                IntraService.byteBufferForObject(params.getString("rowkey")));
        QueryPath qp = new QueryPath(HandlerUtils.determineCf(params,state),
                null,
                IntraService.byteBufferForObject(params.getField("name")));

        Object val = params.getField("value");

        Integer ttl = params.getInteger("ttl");
        if (ttl == null) {
            // TODO add autoTimestamp and nanotime to the state object sent in the event bus message
            rm.add(qp, IntraService.byteBufferForObject(IntraService.resolveObject(val, null, null, null, id)),
                System.nanoTime());
        } else {
            rm.add(qp, IntraService.byteBufferForObject(IntraService.resolveObject(val, null, null, null, id)),
                System.nanoTime(), ttl);
        }
        List<IMutation> mutations = new ArrayList<IMutation>();
        mutations.add(rm);
        HandlerUtils.write(mutations, event, id);
    }
}
