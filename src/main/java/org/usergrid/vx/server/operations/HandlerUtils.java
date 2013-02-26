package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.HashMap;
import java.util.Iterator;

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
          m.put("value", TypeHelper.getTyped(clazz, ic.value()));
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

}
