package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;

/**
 * @author zznate
 */
public class HandlerUtils {

  public static String determineCf(JsonObject params, JsonObject state) {
    String cf = null;
    if (params.getString("columnfamily") != null) {
      cf = params.getString("columnfamily");
    } else {
      cf = state.getString("currentColumnFamily");
    }
    return cf;
  }

  public static String determineKs(JsonObject params, JsonObject state) {
      String ks = null;
      if (params.getString("keyspace") != null) {
          ks = params.getString("keyspace");
      } else {
          ks = state.getString("currentKeyspace");
      }
      return ks;
  }

  public static JsonArray readCf(ColumnFamily columnFamily, JsonObject state, JsonObject params) {
    JsonArray components = state.getArray("components");
    JsonArray array = new JsonArray();
    Iterator<IColumn> it = columnFamily.iterator();
    while (it.hasNext()) {
      IColumn ic = it.next();
      if (ic.isLive()) {
        HashMap m = new HashMap();

        if (components.contains("name")) {
          String clazz = state.getObject("meta").getObject("column").getString("clazz");
          m.put("name", TypeHelper.getTyped(clazz, ic.name()));
        }
        if (components.contains("value")) {
          String clazz = state.getObject("meta").getObject("value").getString("clazz");
          if ( ic instanceof CounterColumn ) {
            m.put("value", ((CounterColumn)ic).total());
          } else {
            m.put("value", TypeHelper.getTyped(clazz, ic.value()));
          }
        }
        if (components.contains("timestamp")) {
          m.put("timestamp", ic.timestamp());
        }
        if (components.contains("markeddelete")) {
          m.put("markeddelete", ic.getMarkedForDeleteAt());
        }
        array.addObject(new JsonObject(m));
      }
    }
    return array;
  }

  public static void write(List<IMutation> mutations, Message<JsonObject> event, Integer id) {
    try {
        // We don't want to hard code the consistency level but letting it slide for
        // since it is also hard coded in IntraState
        StorageProxy.mutate(mutations, ConsistencyLevel.ONE);

        event.reply(new JsonObject().putString(id.toString(), "OK"));
    } catch (WriteTimeoutException | UnavailableException | OverloadedException e) {
        event.reply(new JsonObject()
            .putString("exception", e.getMessage())
            .putString("exceptionId", id.toString()));
    }
  }
  
  
  public static Long getOperationTime(JsonObject operation) {
    JsonObject params = operation.getObject("op");
    Long timeout = params.getLong("timeout");
    if (timeout == null) {
      timeout = 10000L;
    }
    return timeout;
  }
}
