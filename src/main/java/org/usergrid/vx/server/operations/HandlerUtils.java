package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.usergrid.vx.experimental.CompositeTool;
import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.Operations;
import org.usergrid.vx.experimental.TypeHelper;
import org.usergrid.vx.experimental.filter.Filter;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * @author zznate
 */
public class HandlerUtils {

  // todo will this be reloadable?
  public static HandlerUtils instance;
  public static Map<String,Filter> filters = new HashMap<String,Filter>();
  
  private static final String resultMode = "resultMode";
  private static final String consistency = "consitency";
  static {
    instance = new HandlerUtils();
  }

  public HandlerUtils() {

  }

  public Filter getFilter(String name){
    return filters.get(name);
  }
  
  public void putFilter(String name, Filter f){
    filters.put(name, f);
  }
  
  public void activateFilter(JsonObject state, String filterName){
    state.putString("currentFilter", filterName);
  }
  
  public JsonObject getResultMode(JsonObject state){
    return state.getObject(resultMode);
  }
  public void deactivateFilter(JsonObject state){
    state.removeField("currentFilter");
  }
  
  public void activateResultMode(JsonObject state, String keyspace, String columnFamily) {
    state.putObject(resultMode, new JsonObject().putString(Operations.KEYSPACE, keyspace)
            .putString(Operations.COLUMN_FAMILY, columnFamily));
  }
  
  public void deactivateResultMode(JsonObject state){
    state.removeField(resultMode);
  }
  
  public void setConsistencyLevel(JsonObject state, String level){
    state.putString(consistency, level );
  }
  
  public ConsistencyLevel getConsistencyLevel(JsonObject state){
    String cls = state.getString(consistency);
    if (cls != null){
      return ConsistencyLevel.valueOf(cls);
    }
    return ConsistencyLevel.ONE;
  }
  
  /*
   * because handlers can not see the responses of other steps easily anymore we move this logic
   * here. Essentially find all res ref objects and replace them
   */
  public void resolveRefs(JsonObject operation, JsonObject results) {
    JsonObject params = operation.getObject(Operations.OP);
    Set<String> names = params.getFieldNames();
    for (String name : names) {
      Object o = params.getField(name);
      if (o instanceof JsonObject) {
        JsonObject j = (JsonObject) o;
        if (j.getString(Operations.TYPE) != null && j.getString(Operations.TYPE).equals("GETREF")) {
          int refId = j.getObject(Operations.OP).getInteger("resultref");
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

  /* determine the consistency level from the state */
  public ConsistencyLevel determineConsistencyLevel(JsonObject state) {
    return (state.getString("consistency") == null) ? ConsistencyLevel.ONE : ConsistencyLevel
            .valueOf(state.getString("consistency"));
  }

  /* Determine the time for the operation */
  public long determineTimestamp(JsonObject params, JsonObject state, JsonObject row) {
    long timestamp = 0;
    if (row != null && row.getLong(Operations.TIMESTAMP) != null) {
      timestamp = row.getLong(Operations.TIMESTAMP);
    } else if (params.getLong(Operations.TIMESTAMP) != null) {
      timestamp = params.getLong(Operations.TIMESTAMP);
    } else if (state.getBoolean(Operations.AUTOTIMESTAMP) != null
            && state.getBoolean(Operations.AUTOTIMESTAMP) == true) {
      timestamp = System.nanoTime();
    }
    return timestamp;
  }

  /*
   * Determine columnfamily first look in the row for a string named keyspace, then look in the op,
   * then look in the state. The row is only currently provided in batchset
   */
  public String determineCf(JsonObject params, JsonObject state, JsonObject row) {
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
  public String determineKs(JsonObject params, JsonObject state, JsonObject row) {
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

  
  public JsonArray readCf(ColumnFamily columnFamily, JsonObject state) {
    return internalCfRead(columnFamily, state);
  }
  

  public JsonObject findMetaData(ColumnFamily cf, JsonObject state, String type) {
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

  public JsonObject findRangedMetaData(ColumnFamily cf, JsonObject state, ByteBuffer name) {
    StringBuilder key = new StringBuilder();
    key.append(cf.metadata().ksName);
    key.append(' ');
    key.append(cf.metadata().cfName);
    key.append(' ');
    key.append(ByteBufferUtil.bytesToHex(name));
    String skey = key.toString();
    JsonObject meta = state.getObject("metaRanged");
    if (meta == null) {
      return null;
      // TODO why is this.
    }
    Set<String> names = meta.getFieldNames();
    for (String s : names) {
      // System.out.println("compare "+skey+ " to "+s);
      if (skey.compareTo(s) > -1) {
        // System.out.println(skey+ " greater then "+s);
        JsonObject value = meta.getObject(s);
        String end = cf.metadata().ksName + ' ' + cf.metadata().cfName + ' '
                + value.getString("end");
        // System.out.println("compare "+skey+ " to "+end);
        if (skey.compareToIgnoreCase(end) < 0) {
          // System.out.println("matched!");
          return value;
        }
      }
    }
    // System.out.println();
    return null;
  }

  public JsonObject findColumnMetaData(ColumnFamily cf, JsonObject state, ByteBuffer name) {
    StringBuilder key = new StringBuilder();
    key.append(cf.metadata().ksName);
    key.append(' ');
    key.append(cf.metadata().cfName);
    key.append(' ');
    key.append(ByteBufferUtil.bytesToHex(name));
    JsonObject meta = state.getObject("metaColumn");
    if (meta != null) {
      return meta.getObject(key.toString());
    } else {
      return null;
    }
  }

  public JsonArray internalCfRead(ColumnFamily columnFamily, JsonObject state){
    JsonArray components = state.getArray("components");
    JsonArray array = new JsonArray();
    Iterator<IColumn> it = columnFamily.iterator();
    while (it.hasNext()) {
      IColumn column = it.next();
      if (column.isLive()) {
        HashMap<String,Object> m = new HashMap<>(4);
        if (components.contains("name")) {
          JsonObject columnMetadata = findMetaData(columnFamily, state, "column");
          if (columnMetadata == null) {
            m.put("name", ByteBufferUtil.getArray(column.name()));
          } else {
            String clazz = columnMetadata.getString("clazz");
            m.put("name", TypeHelper.getTyped(clazz, column.name()));
            Object name = TypeHelper.getTyped(clazz, column.name());
            if (name instanceof ByteBuffer) {
              m.put("name", TypeHelper.getBytes(column.name()));
            } else {
              m.put("name", TypeHelper.getTyped(clazz, column.name()));
            }
          }
        }
        if (components.contains("value")) {
          if (column instanceof CounterColumn) {
            m.put("value", ((CounterColumn) column).total());
          } else {
            JsonObject valueMetaData = findColumnMetaData(columnFamily, state, column.name()
                    .duplicate());
            if (valueMetaData == null) {
              valueMetaData = findMetaData(columnFamily, state, "value");
            }
            if (valueMetaData == null) {
              valueMetaData = findRangedMetaData(columnFamily, state, column.name().duplicate());
            }
            if (valueMetaData == null) {
              m.put("value", ByteBufferUtil.getArray(column.value()));
            } else {
              String clazz = valueMetaData.getString("clazz");
              Object value = TypeHelper.getTyped(clazz, column.value());
              if (value instanceof ByteBuffer) {
                m.put("value", TypeHelper.getBytes(column.value()));
              } else {
                m.put("value", value);
              }
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
    return array;
  }
  
  public void readCf(ColumnFamily columnFamily, JsonObject state, EventBus eb,
          Handler<Message<JsonArray>> filterReplyHandler) {
    JsonArray array = internalCfRead(columnFamily, state);
    String filter = state.getString("currentFilter");
    eb.send("filters." + filter, array, filterReplyHandler);
  }

  public void write(List<IMutation> mutations, Message<JsonObject> event, Integer id, JsonObject state) {
    try {
      StorageProxy.mutate(mutations, ConsistencyLevel.ONE);

      event.reply(new JsonObject().putString(id.toString(), "OK"));
    } catch (WriteTimeoutException | UnavailableException | OverloadedException e) {
      event.reply(new JsonObject().putString("exception", e.getMessage()).putString("exceptionId",
              id.toString()));
    }
  }

  public Long getOperationTimeout(JsonObject operation) {
    JsonObject params = operation.getObject(Operations.OP);
    Long timeout = params.getLong(Operations.TIMEOUT);
    if (timeout == null) {
      timeout = 10000L;
    }
    return timeout;
  }

  public JsonObject buildError(Integer id, String errorMessage) {
    return new JsonObject().putString("exception", errorMessage).putNumber("exceptionId", id);
  }

  public ByteBuffer byteBufferForObject(Object o) {
    if (o instanceof Object[]) {
      Object[] comp = (Object[]) o;
      List<byte[]> b = new ArrayList<byte[]>();
      int[] sep = new int[comp.length / 2];
      for (int i = 0; i < comp.length; i = i + 2) {
        // get the element
        ByteBuffer element = byteBufferForObject(comp[i]);
        byte[] by = new byte[element.remaining()];
        element.get(by);
        b.add(by);
        // this part must be an unsigned int
        sep[i / 2] = (Integer) comp[i + 1];
      }
      byte[] entireComp = CompositeTool.makeComposite(b, sep);
      return ByteBuffer.wrap(entireComp);
    } else if (o instanceof Integer) {
      return Int32Type.instance.decompose((Integer) o);
      // return ByteBufferUtil.bytes( ((Integer) o).intValue());
    } else if (o instanceof String) {
      return ByteBufferUtil.bytes((String) o);
    } else if (o instanceof byte[]) {
      return ByteBuffer.wrap((byte[]) o);
    } else if (o instanceof ByteBuffer) {
      return (ByteBuffer) o;
    } else
      throw new RuntimeException("can not serializer " + o);
  }

  public Object resolveObject(Object o) {
    if (o instanceof JsonArray) {
      return ((JsonArray) o).toArray();
    } else if (o instanceof ArrayList) {
      return ((ArrayList) o).toArray();
    } else if (o instanceof Object[]) {
      return o;
    } else if (o instanceof Integer) {
      return o;
    } else if (o instanceof String) {
      return o;
    } else if (o instanceof Map) {
      Map<String, Object> map = (Map<String, Object>) o;
      Object typeAttr = map.get("type");
      if (isGetRef(typeAttr)) {
        Map<String, Object> op = (Map<String, Object>) map.get("op");
        Integer resultRef = (Integer) op.get("resultref");
        String wanted = (String) op.get("wanted");
        /*
         * List referencedResult = (List) res.getOpsRes().get(resultRef); Map result = (Map)
         * referencedResult.get(0); return result.get(wanted);
         */
        return null;
      } else if (isBind(typeAttr)) {
        Integer mark = (Integer) map.get("mark");
        return null;
        // return state.bindParams.get(mark);
      } else {
        throw new IllegalArgumentException("Do not know what todo with " + o);
      }
    } else if (o instanceof IntraOp) {
      IntraOp op = (IntraOp) o;
      throw new RuntimeException(" do not know what to do with " + op.getType());
    } else {
      throw new RuntimeException(" do not know what to do with" + o.getClass());
    }
  }

  private boolean isGetRef(Object typeAttr) {
    return typeAttr != null && typeAttr instanceof String && typeAttr.equals("GETREF");
  }

  private boolean isBind(Object typeAttr) {
    return typeAttr != null && typeAttr instanceof String && typeAttr.equals("BINDMARKER");
  }

}
