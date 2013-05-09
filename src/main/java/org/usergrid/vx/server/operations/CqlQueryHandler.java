package org.usergrid.vx.server.operations;

import org.apache.cassandra.cql3.ColumnSpecification;
import org.apache.cassandra.cql3.QueryProcessor;
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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class CqlQueryHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");
    ClientState clientState = new ClientState();
    try {
      clientState.setCQLVersion(params.getString("version"));
      clientState.setKeyspace(HandlerUtils.instance.determineKs(params, state, null));
    } catch (InvalidRequestException e) {
      event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
      return;
    }
    QueryState queryState = new QueryState(clientState);
    ResultMessage rm = null;
    try {
      rm = QueryProcessor.process(params.getString("query"), HandlerUtils.instance.determineConsistencyLevel(state), queryState);
    } catch (RequestExecutionException | RequestValidationException e) {
      event.reply(new JsonObject()
          .putString(id.toString(), "ERROR")
          .putString("exceptionId", id.toString())
          .putString("exception", e.getMessage()));
      return;
    }
    List<Map<String,Object>> returnRows = new ArrayList<>();
    if (rm.kind == ResultMessage.Kind.ROWS) {
      //ToDo maybe processInternal
      if (params.getBoolean("transpose", true)) {
        ResultMessage.Rows cqlRows = (ResultMessage.Rows) rm;
        List<ColumnSpecification> columnSpecs = cqlRows.result.metadata.names;

        for (List<ByteBuffer> row : cqlRows.result.rows) {
          Map<String,Object> map = new HashMap<String,Object>();
          int i = 0;
          for (ByteBuffer bytes : row) {
            ColumnSpecification specs = columnSpecs.get(i++);
            map.put(specs.name.toString(), specs.type.compose(bytes));
          }
          returnRows.add(map);
        }
      } else {
        boolean convert = params.getBoolean("convert") != null;
        CqlResult result = rm.toThriftResult();
        List<CqlRow> rows = result.getRows();
        for (CqlRow row: rows) {
          List<org.apache.cassandra.thrift.Column> columns = row.getColumns();
          for (org.apache.cassandra.thrift.Column c: columns){
            HashMap<String,Object> m = new HashMap<String,Object>();
            if (convert) {
              m.put("name" , TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.name) );
              m.put("value" , TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.value) );
            } else {
              m.put("value", TypeHelper.getBytes(c.value));
              m.put("name", TypeHelper.getBytes(c.name));
            }
            returnRows.add(m);
          }
        }
      }
    }
    JsonObject response = new JsonObject();
    JsonArray array = new JsonArray();
    for (Map<String,Object> m : returnRows) {
      array.add(new JsonObject(m));
    }
    response.putString(id.toString(), "OK");
    response.putArray(id.toString(), array);
    event.reply(response);
  }
}
