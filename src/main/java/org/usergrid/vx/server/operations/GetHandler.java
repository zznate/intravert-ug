package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.usergrid.vx.experimental.IntraService;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

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
    QueryPath path = new QueryPath(HandlerUtils.determineCf(params, state), null);
    List<ByteBuffer> nameAsList = Arrays.asList(column);
    ReadCommand command = new SliceByNamesReadCommand(HandlerUtils.determineKs(params, state), rowkey, path, nameAsList);
    List<Row> rows = null;

    try {
      // We don't want to hard code the consistency level but letting it slide for
      // since it is also hard coded in IntraState
      rows = StorageProxy.read(Arrays.asList(command), ConsistencyLevel.ONE);
      ColumnFamily cf1 = rows.get(0).cf;
      JsonArray array;
      if (cf1 == null) { // cf= null is no data
        array = new JsonArray();
      } else {
        array = HandlerUtils.readCf(cf1, state, params);
      }

      System.out.println(array.toArray());
      JsonObject response = new JsonObject();
      response.putArray(id.toString(), array);
      event.reply(response);
    } catch (ReadTimeoutException | UnavailableException | IsBootstrappingException | IOException e) {
      event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
    }
  }

}
