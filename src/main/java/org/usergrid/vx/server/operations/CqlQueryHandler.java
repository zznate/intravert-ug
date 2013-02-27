package org.usergrid.vx.server.operations;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.transport.messages.ResultMessage;
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
            clientState.setKeyspace(HandlerUtils.determineKs(params,state));
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
            event.reply(new JsonObject()
                .putString(id.toString(), "ERROR")
                .putString("exceptionId", id.toString())
                .putString("exception", e.getMessage()));
            return;
        }
        List<Map> returnRows = new ArrayList<>();
        if (rm.kind == ResultMessage.Kind.ROWS) {
            //ToDo maybe processInternal
            ResultMessage.Rows cqlRows = (ResultMessage.Rows) rm;
            List<ColumnSpecification> columnSpecs = cqlRows.result.metadata.names;

            for (List<ByteBuffer> row : cqlRows.result.rows) {
                Map map = new HashMap();
                int i = 0;
                for (ByteBuffer bytes : row) {
                    ColumnSpecification specs = columnSpecs.get(i++);
                    map.put(specs.name.toString(), specs.type.compose(bytes));
                }
                returnRows.add(map);
            }
        }
        JsonObject response = new JsonObject();
        JsonArray array = new JsonArray();
        for (Map m : returnRows) {
            array.add(new JsonObject(m));
        }
        response.putString(id.toString(), "OK");
        response.putArray(id.toString(), array);
        event.reply(response);
    }
}
