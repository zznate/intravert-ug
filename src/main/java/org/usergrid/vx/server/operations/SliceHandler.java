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
import java.util.List;
import java.util.Map;

public class SliceHandler implements Handler<Message<JsonObject>> {

  @Override
  public void handle(Message<JsonObject> event) {
    Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    List<Map> finalResults = new ArrayList<Map>();
    Map<String, Object> paramsMap = params.toMap();
    Object rowKeyParam = paramsMap.get("rowkey");
    Object startParam = paramsMap.get("start");
    Object endParam = paramsMap.get("end");

    ByteBuffer rowkey = IntraService
        .byteBufferForObject(IntraService.resolveObject(rowKeyParam, null, null, null, id));
    ByteBuffer start = IntraService.byteBufferForObject(IntraService.resolveObject(startParam, null, null, null, id));
    ByteBuffer end = IntraService.byteBufferForObject(IntraService.resolveObject(endParam, null, null, null, id));

    List<ReadCommand> commands = new ArrayList<ReadCommand>(1);

    QueryPath path = new QueryPath(HandlerUtils.determineCf(params, state), null);

    SliceFromReadCommand sr = new SliceFromReadCommand(state.getString("currentKeyspace"), rowkey, path, start, end,
        false, 100);
    commands.add(sr);

    List<Row> results = null;
    try {
      // We don't want to hard code the consistency level but letting it slide for
      // since it is also hard coded in IntraState
      results = StorageProxy.read(commands, ConsistencyLevel.ONE);
      ColumnFamily cf = results.get(0).cf;
      JsonArray array;

      if (cf == null) { //cf= null is no data
        array  = new JsonArray();
      } else {
        array = HandlerUtils.readCf(cf, state, params);
      }

      JsonObject response = new JsonObject();
      response.putArray(id.toString(), array);
      event.reply(response);
    } catch (ReadTimeoutException | UnavailableException | IsBootstrappingException | IOException e) {
      event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
    }
  }

}
