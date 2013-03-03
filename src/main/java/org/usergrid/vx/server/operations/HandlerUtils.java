package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
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
          JsonObject columnMetadata = state.getObject("meta").getObject("column");
          if (columnMetadata == null) {
            m.put("name", TypeHelper.getBytes(ic.name()));
          } else {
            String clazz = columnMetadata.getString("clazz");
            Object name = TypeHelper.getTyped(clazz, ic.name());
            if (name instanceof ByteBuffer) {
              m.put("name", TypeHelper.getBytes(ic.name()));
            } else {
              m.put("name", TypeHelper.getTyped(clazz, ic.name()));
            }
          }
        }
        if (components.contains("value")) {
          if ( ic instanceof CounterColumn ) {
            m.put("value", ((CounterColumn)ic).total());
          } else {
            JsonObject valueMetadata = state.getObject("meta").getObject("value");
            if (valueMetadata == null) {
              m.put("value", TypeHelper.getBytes(ic.value()));
            } else {
              String clazz = valueMetadata.getString("clazz");
              Object value = TypeHelper.getTyped(clazz, ic.value());
              if (value instanceof ByteBuffer) {
                m.put("value", TypeHelper.getBytes(ic.value()));
              } else {
                m.put("value", value);
              }
            }
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

public static void readCf(ColumnFamily columnFamily, JsonObject state, EventBus eb,
    Handler<Message<JsonArray>> filterReplyHandler) {
    JsonArray components = state.getArray("components");
    JsonArray array = new JsonArray();

    for (IColumn column : columnFamily) {
      if (column.isLive()) {
        HashMap m = new HashMap();

        if (components.contains("name")) {
          JsonObject columnMetadata = state.getObject("meta").getObject("column");
          if (columnMetadata == null) {
            m.put("name", ByteBufferUtil.getArray(column.name()));
          } else {
            String clazz = columnMetadata.getString("clazz");
            m.put("name", TypeHelper.getTyped(clazz, column.name()));
          }
        }
        if (components.contains("value")) {
          if (column instanceof CounterColumn ) {
            m.put("value", ((CounterColumn)column).total());
          } else {
            JsonObject valueMetadata = state.getObject("meta").getObject("value");
            if (valueMetadata == null) {
              m.put("value", ByteBufferUtil.getArray(column.value()));
            } else {
              String clazz = valueMetadata.getString("clazz");
              m.put("value", TypeHelper.getTyped(clazz, column.value()));
            }
          }
        }
        if (components.contains("timestamp")) {
          m.put("timestamp", column.timestamp());
        }
        if (components.contains("markeddelete")) {
          m.put("markeddelete", column.getMarkedForDeleteAt());
        }
        array.addObject(new JsonObject(m));
      }
    }

    String filter = state.getString("currentFilter");
    eb.send("filters." + filter, array, filterReplyHandler);
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
