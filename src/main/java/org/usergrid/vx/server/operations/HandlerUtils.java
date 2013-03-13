package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zznate
 */
public class HandlerUtils {

  /*
   * because handlers can not see the responses of other steps easily anymore we move this logic
   * here. Essentially find all res ref objects and replace them
   */
  public static void resolveRefs(JsonObject operation, JsonObject results) {
    JsonObject params = operation.getObject("op");
    Set<String> names = params.getFieldNames();
    for (String name : names) {
      Object o = params.getField(name);
      if (o instanceof JsonObject) {
        JsonObject j = (JsonObject) o;
        if (j.getString("type") != null && j.getString("type").equals("GETREF")) {
          int refId = j.getObject("op").getInteger("resultref");
          String wanted = j.getObject("op").getString("wanted");
          Object k = results.getArray(refId + "").get(0);
          JsonObject m = (JsonObject) k;
          Object theDamnThing = m.getField(wanted);
          if (theDamnThing instanceof String) {
            params.putString(name, (String) theDamnThing);
          }
          if (theDamnThing instanceof Number) {
            params.putNumber(name, (Number) theDamnThing);
          }
        }
      }
    }
  }

  /*
   * Determine columnfamily first look in the row for a string named keyspace, then look in the op,
   * then look in the state. The row is only currently provided in batchset
   */
  public static String determineCf(JsonObject params, JsonObject state, JsonObject row) {
    String cf = null;
    if (row != null && row.getString("columnfamily") != null) {
      cf = row.getString("columnfamily");
    } else if (params.getString("columnfamily") != null) {
      cf = params.getString("columnfamily");
    } else {
      cf = state.getString("currentColumnFamily");
    }
    return cf;
  }

  /*
   * Determine keyspace first look in the row for a string named keyspace, then look in the op, then
   * look in the state. The row is only currently provided in batchset
   */
  public static String determineKs(JsonObject params, JsonObject state, JsonObject row) {
    String ks = null;
    if (row != null && row.getString("keyspace") != null) {
      ks = row.getString("keyspace");
    } else if (params.getString("keyspace") != null) {
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
          JsonObject columnMetadata = findMetaData(columnFamily, state, "column");
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
          if (ic instanceof CounterColumn) {
            m.put("value", ((CounterColumn) ic).total());
          } else {
            JsonObject valueMetadata = findMetaData(columnFamily, state, "value");
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

  public static JsonObject findMetaData(ColumnFamily cf, JsonObject state, String type) {
    JsonObject meta = state.getObject("meta");
    if (meta == null) {
      return null;
    } else {
      StringBuilder key = new StringBuilder();
      key.append(cf.metadata().ksName);
      key.append(' ');
      key.append(cf.metadata().cfName);
      key.append(' ');
      key.append(type);
      return meta.getObject(key.toString());
    }
  }

  public static void readCf(ColumnFamily columnFamily, JsonObject state, EventBus eb,
          Handler<Message<JsonArray>> filterReplyHandler) {
    JsonArray components = state.getArray("components");
    JsonArray array = new JsonArray();

    for (IColumn column : columnFamily) {
      if (column.isLive()) {
        HashMap m = new HashMap();

        if (components.contains("name")) {
          JsonObject columnMetadata = findMetaData(columnFamily, state, "column");
          if (columnMetadata == null) {
            m.put("name", ByteBufferUtil.getArray(column.name()));
          } else {
            String clazz = columnMetadata.getString("clazz");
            m.put("name", TypeHelper.getTyped(clazz, column.name()));
          }
        }
        if (components.contains("value")) {
          if (column instanceof CounterColumn) {
            m.put("value", ((CounterColumn) column).total());
          } else {
            JsonObject valueMetadata = findMetaData(columnFamily, state, "value");
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
      event.reply(new JsonObject().putString("exception", e.getMessage()).putString("exceptionId",
              id.toString()));
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

  public static JsonObject buildError(Integer id, String errorMessage) {
    return new JsonObject().putString("exception", errorMessage).putNumber("exceptionId", id);
  }
}
