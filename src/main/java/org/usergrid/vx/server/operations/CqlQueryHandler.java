package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class CqlQueryHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        Integer id = event.body.getInteger("id");
        JsonObject params = event.body.getObject("op");
        JsonObject state = event.body.getObject("state");

        ClientState clientState = new ClientState();
        try {
            clientState.setCQLVersion(params.getString("version"));
            clientState.setKeyspace(state.getString("currentKeyspace"));
        } catch (InvalidRequestException e) {
            event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
            return;
        }
        QueryState queryState = new QueryState(clientState);
        ResultMessage rm = null;
        try {
            // We don't want to hard code the consistency level but letting it slide for
            // since it is also hard coded in IntraState
            rm = QueryProcessor.process(params.getString("query"), ConsistencyLevel.ONE, queryState);
        } catch (RequestExecutionException | RequestValidationException e) {
            event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
            return;
        }
        List<HashMap> returnRows = new ArrayList<HashMap>();
        if (rm.kind == ResultMessage.Kind.ROWS) {
            //ToDo maybe processInternal
            CqlResult result = rm.toThriftResult();
            List<CqlRow> rows = result.getRows();
            for (CqlRow row : rows) {
                List<org.apache.cassandra.thrift.Column> columns = row.getColumns();
                for (org.apache.cassandra.thrift.Column c : columns) {
                    HashMap m = new HashMap();
                    if (params.getString("convert") != null) {
                        m.put("name", TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.name));
                        m.put("value", TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.value));
                    } else {
                        m.put("value", c.value);
                        m.put("name", c.name);
                    }
                    returnRows.add(m);
                }
            }
        }
        JsonObject response = new JsonObject();
        JsonArray array = new JsonArray();
        for (Map m : returnRows) {
            array.add(new JsonObject(m));
        }
        response.putArray(id.toString(), array);
        event.reply(response);
    }
}
