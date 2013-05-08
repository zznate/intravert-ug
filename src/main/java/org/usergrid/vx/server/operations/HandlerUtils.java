package org.usergrid.vx.server.operations;

import org.apache.cassandra.db.*;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.map.ObjectMapper;
import org.usergrid.vx.client.IntraClient2;
import org.usergrid.vx.experimental.CompositeTool;
import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.IntraRes;
import org.usergrid.vx.experimental.IntraState;
import org.usergrid.vx.experimental.NonAtomicReference;
import org.usergrid.vx.experimental.Operations;
import org.usergrid.vx.experimental.TypeHelper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonElement;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;

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

  /*determine the consistency level from the state */
  public static ConsistencyLevel determineConsistencyLevel(JsonObject state){
    return (state.getString("consistency") == null) ? 
            ConsistencyLevel.ONE 
            : ConsistencyLevel.valueOf(state.getString("consistency"));
  }
  
  /*Determine the time for the operation */
  public static long determineTimestamp(JsonObject params, JsonObject state, JsonObject row){
    long timestamp = 0;
    if (row != null && row.getLong("timestamp") != null){
      timestamp = row.getLong("timestamp");
    } else if (params.getLong("timestamp") != null){
      timestamp = params.getLong("timestamp"); 
    } else if (state.getBoolean("autotimestamp")!= null && state.getBoolean("autotimestamp") == true){
      timestamp = System.nanoTime();
    }
    return timestamp;
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
    System.out.println("state1"+ state);
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
            JsonObject valueMetadata = HandlerUtils.findColumnMetaData(columnFamily, state, ic.name().duplicate());
            if (valueMetadata == null){
              valueMetadata = findMetaData(columnFamily, state, "value");
            }
            if (valueMetadata == null){
              valueMetadata = findRangedMetaData(columnFamily, state, ic.name().duplicate());
            }
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
  public static JsonObject findRangedMetaData(ColumnFamily cf, JsonObject state, ByteBuffer name) {
    //System.out.println();
    //System.out.println("ranged meta data");
    StringBuilder key = new StringBuilder();
    key.append(cf.metadata().ksName);
    key.append(' ');
    key.append(cf.metadata().cfName);
    key.append(' ');
    key.append(ByteBufferUtil.bytesToHex(name));
    String skey = key.toString();
    JsonObject meta = state.getObject("metaRanged");
    if (meta==null){
      return null;
      //TODO why is this.
    }
    Set<String> names = meta.getFieldNames();
    for (String s: names){
      //System.out.println("compare "+skey+ " to "+s);
      if (skey.compareTo(s)>-1){
        //System.out.println(skey+ " greater then "+s);
        JsonObject value = meta.getObject(s);
        String end = cf.metadata().ksName+' '+cf.metadata().cfName+' '+value.getString("end");
        //System.out.println("compare "+skey+ " to "+end);
        if (skey.compareToIgnoreCase(end)<0){
          //System.out.println("matched!");
          return value;
        }
      }
    }
    //System.out.println();
    return null;
  }
  
  public static JsonObject findColumnMetaData(ColumnFamily cf, JsonObject state, ByteBuffer name) {
    StringBuilder key = new StringBuilder();
    key.append(cf.metadata().ksName);
    key.append(' ');
    key.append(cf.metadata().cfName);
    key.append(' ');
    key.append(ByteBufferUtil.bytesToHex(name));
    JsonObject meta = state.getObject("metaColumn");
    if (meta!=null) {
      return meta.getObject(key.toString());
    } else {
      return null;
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
            JsonObject valueMetaData = HandlerUtils.findColumnMetaData(columnFamily, state, column.name().duplicate());
            if (valueMetaData == null){
              valueMetaData = findMetaData(columnFamily, state, "value");
            } 
            if (valueMetaData == null){
              valueMetaData = findRangedMetaData(columnFamily, state, column.name().duplicate());
            }
            if (valueMetaData == null) {
              m.put("value", ByteBufferUtil.getArray(column.value()));
            } else {
              String clazz = valueMetaData.getString("clazz");
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

  public static Long getOperationTimeout(JsonObject operation) {
    JsonObject params = operation.getObject(Operations.OP);
    Long timeout = params.getLong(Operations.TIMEOUT);
    if (timeout == null) {
      timeout = 10000L;
    }
    return timeout;
  }

  public static JsonObject buildError(Integer id, String errorMessage) {
    return new JsonObject().putString("exception", errorMessage).putNumber("exceptionId", id);
  }
  
  public static ByteBuffer byteBufferForObject(Object o) {
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
  
  
  public static Object resolveObject(Object o) {
    if (o instanceof JsonArray) {
      return ((JsonArray) o).toArray();
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
        List referencedResult = (List) res.getOpsRes().get(resultRef);
        Map result = (Map) referencedResult.get(0);
        return result.get(wanted);
        */
        return null;
      } else if (isBind(typeAttr)) {
        Integer mark = (Integer) map.get("mark");
        return null;
        //return state.bindParams.get(mark);
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

  private static boolean isGetRef(Object typeAttr) {
    return typeAttr != null && typeAttr instanceof String && typeAttr.equals("GETREF");
  }

  private static boolean isBind(Object typeAttr) {
    return typeAttr != null && typeAttr instanceof String && typeAttr.equals("BINDMARKER");
  }

}
