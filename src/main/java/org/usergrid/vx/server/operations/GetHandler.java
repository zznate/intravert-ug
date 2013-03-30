package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

public class GetHandler implements Handler<Message<JsonObject>> {

  private EventBus eb;

  public GetHandler(EventBus eb) {
    this.eb = eb;
  }

  @Override
  public void handle(final Message<JsonObject> event) {
    final Integer id = event.body.getInteger("id");
    JsonObject params = event.body.getObject("op");
    JsonObject state = event.body.getObject("state");

    Map<String, Object> paramsMap = params.toMap();
    Object rowKeyParam = paramsMap.get("rowkey");
    Object nameParam = paramsMap.get("name");

    ByteBuffer rowkey = HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(rowKeyParam));
    ByteBuffer column = HandlerUtils.byteBufferForObject(HandlerUtils.resolveObject(nameParam));
    QueryPath path = new QueryPath(HandlerUtils.determineCf(params, state, null), null);
    List<ByteBuffer> nameAsList = Arrays.asList(column);
    ReadCommand command = new SliceByNamesReadCommand(HandlerUtils.determineKs(params, state, null), rowkey, path, nameAsList);
    List<Row> rows = null;

    try {
      // We don't want to hard code the consistency level but letting it slide for
      // since it is also hard coded in IntraState
      rows = StorageProxy.read(Arrays.asList(command), ConsistencyLevel.ONE);
      ColumnFamily cf = rows.get(0).cf;
      new ReadHandler(event, eb).handleRead(cf);
    } catch (ReadTimeoutException | UnavailableException | IsBootstrappingException | IOException e) {
      event.reply(new JsonObject().putString(id.toString(), e.getMessage()));
    }
  }

}
