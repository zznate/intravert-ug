package org.usergrid.vx.server.operations;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.usergrid.vx.experimental.IntraService;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class GetHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        Integer id = event.body.getInteger("id");
        JsonObject params = event.body.getObject("op");
        JsonObject state = event.body.getObject("state");

        Map<String, Object> paramsMap = params.toMap();
        Object rowKeyParam = paramsMap.get("rowkey");
        Object nameParam = paramsMap.get("name");
        List<Map> finalResults = new ArrayList<Map>();

        ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(rowKeyParam, null, null, null,
            id));
        ByteBuffer column = IntraService.byteBufferForObject(IntraService.resolveObject(nameParam, null, null, null,
            id));
        QueryPath path = new QueryPath(determineCf(params, state), null);
        List<ByteBuffer> nameAsList = Arrays.asList(column);
        ReadCommand command = new SliceByNamesReadCommand(determineKs(params, state), rowkey, path, nameAsList);
        List<Row> rows = null;

        try {
            // We don't want to hard code the consistency level but letting it slide for
            // since it is also hard coded in IntraState
            rows = StorageProxy.read(Arrays.asList(command), ConsistencyLevel.ONE);
            ColumnFamily cf1 = rows.get(0).cf;
            if (cf1 == null) { // cf= null is no data
            } else {
                readCf(cf1, finalResults, state, params);
            }

            JsonObject response = new JsonObject();
            JsonArray array = new JsonArray();
            for (Map m : finalResults) {
                array.add(new JsonObject(m));
            }
            response.putArray(id.toString(), array);
            event.reply(response);
        } catch (ReadTimeoutException | UnavailableException | IsBootstrappingException | IOException e) {
            event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
        }
    }

    private String determineKs(JsonObject params, JsonObject state) {
        String ks = null;
        if (params.getString("keyspace") != null) {
            ks = params.getString("keyspace");
        } else {
            ks = state.getString("currentKeyspace");
        }
        return ks;
    }

    private String determineCf(JsonObject params, JsonObject state) {
        String cf = null;
        if (params.getString("columnfamily") != null) {
            cf = params.getString("columnfamily");
        } else {
            cf = state.getString("currentColumnFamily");
        }
        return cf;
    }

    private void readCf(ColumnFamily columnFamily, List<Map> finalResults, JsonObject state, JsonObject params) {
        Iterator<IColumn> it = columnFamily.iterator();
        while (it.hasNext()) {
            IColumn ic = it.next();
            if (ic.isLive()) {
                HashMap m = new HashMap();
                JsonArray components = state.getArray("components");

                if (components.contains("name")) {
                    String clazz = state.getObject("meta").getObject("column").getString("clazz");
                    m.put("name", TypeHelper.getTypedIfPossible(clazz, ic.name()));
                }
                if (components.contains("value")) {
                    String clazz = state.getObject("meta").getObject("value").getString("clazz");
                    m.put("value", TypeHelper.getTypedIfPossible(clazz, ic.value()));
                }
                if (components.contains("timestamp")) {
                    m.put("timestamp", ic.timestamp());
                }
                if (components.contains("markeddelete")) {
                    m.put("markeddelete", ic.getMarkedForDeleteAt());
                }
//                if (state.currentFilter != null) {
//                    Map newMap = (Map) state.currentFilter.filter(m);
//                    if (newMap != null) {
//                        finalResults.add(newMap);
//                    }
//                } else {
//                    finalResults.add(m);
//                }
                finalResults.add(m);
            }
        }
    }
}
