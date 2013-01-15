package org.usergrid.vx.experimental;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.ConsistencyLevel;

import java.util.List;
import java.util.Map;

/**
 * Factory class for building IntraOp objects
 * @author zznate
 */
public class Operations {
  private Operations() {}

  public static IntraOp setKeyspaceOp(String keyspace){
    checkForBlankStr(keyspace, "keyspace", IntraOp.Type.SETKEYSPACE);
    return new IntraOp(IntraOp.Type.SETKEYSPACE)
            .set("keyspace", keyspace);
  }

  public static IntraOp setColumnFamilyOp(String columnFamily){
    checkForBlankStr(columnFamily, "columnFamily", IntraOp.Type.SETCOLUMNFAMILY);
 		return new IntraOp(IntraOp.Type.SETCOLUMNFAMILY)
             .set("columnfamily", columnFamily);
 	}

  public static IntraOp setAutotimestampOp(){
 		return new IntraOp(IntraOp.Type.AUTOTIMESTAMP);
 	}

  public static IntraOp setOp(Object rowkey, Object columnName, Object columnValue){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", IntraOp.Type.SET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", IntraOp.Type.SET);
    Preconditions.checkArgument(columnValue != null, "Cannot set a column to null for {}", IntraOp.Type.SET);
    return new IntraOp(IntraOp.Type.SET)
            .set("rowkey", rowkey)
            .set("name", columnName)
            .set("value", columnValue);
 	}

  public static IntraOp batchSetOp(List<Map> rows){
 	  return new IntraOp(IntraOp.Type.BATCHSET).set("rows", rows);
 	}

  public static IntraOp getOp(Object rowkey, Object columnName){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", IntraOp.Type.GET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", IntraOp.Type.GET);
 		return new IntraOp(IntraOp.Type.GET)
             .set("rowkey", rowkey)
             .set("name", columnName);
 	}

  public static IntraOp getResRefOp(int reference, String wanted){
    Preconditions.checkArgument(reference >= 0);
    checkForBlankStr(wanted, "wanted", IntraOp.Type.GETREF);
    return new IntraOp(IntraOp.Type.GETREF)
            .set("resultref", reference)
            .set("wanted", wanted);
 	}

  public static IntraOp sliceOp( Object rowkey , Object start, Object end, int size){
    Preconditions.checkArgument(rowkey != null,"A row key is required for {}", IntraOp.Type.SLICE);
    Preconditions.checkArgument(size > 0, "A slice size must be positive integer for {}", IntraOp.Type.SLICE);
    return new IntraOp(IntraOp.Type.SLICE)
            .set("rowkey", rowkey)
            .set("start", start)
            .set("end", end)
            .set("size", size);
  }

  public static IntraOp columnPredicateOp( Object rowkey, Object [] columnList){
    Preconditions.checkArgument(columnList != null, "You much provide a columnList array");
 		return new IntraOp(IntraOp.Type.COLUMNPREDICATE)
             .set("wantedcols", columnList);
 	}

  public static IntraOp forEachOp( int opRef, IntraOp action){
    Preconditions.checkArgument(action != null, "The IntraOp action cannot be null");
    return new IntraOp(IntraOp.Type.FOREACH)
            .set("action", action);
  }

 	public static IntraOp createCfOp(String cfName){
    checkForBlankStr(cfName, "columnFamily name", IntraOp.Type.CREATECOLUMNFAMILY);
 		return new IntraOp(IntraOp.Type.CREATECOLUMNFAMILY)
             .set("name", cfName);
 	}

  public static IntraOp createKsOp(String ksname, int replication){
    checkForBlankStr(ksname, "keyspace name", IntraOp.Type.CREATEKEYSPACE);
    Preconditions.checkArgument(replication > 0,
            "A value for positive value for 'replication' is required for {}", IntraOp.Type.CREATEKEYSPACE);
    return new IntraOp(IntraOp.Type.CREATEKEYSPACE)
            .set("name", ksname)
            .set("replication", replication);
  }

  public static IntraOp consistencyOp(String name){
     // Force an IllegalArgumentException
    ConsistencyLevel.valueOf(name);
 		return new IntraOp(IntraOp.Type.CONSISTENCY)
             .set("level", name);
 	}

 	public static IntraOp listKeyspacesOp(){
 	  return new IntraOp(IntraOp.Type.LISTKEYSPACES);
 	}

  public static IntraOp listColumnFamilyOp(String keyspace){
    checkForBlankStr(keyspace, "Keyspace name", IntraOp.Type.LISTCOLUMNFAMILY);
    return new IntraOp(IntraOp.Type.LISTCOLUMNFAMILY)
            .set("keyspace", keyspace);
  }

  public static IntraOp assumeOp(String keyspace,String columnfamily,String type, String clazz){
    return new IntraOp(IntraOp.Type.ASSUME)
            .set("keyspace", keyspace)
            .set("columnfamily", columnfamily)
            .set("type", type) //should be column rowkey value
            .set("clazz", clazz );
  }

  public static IntraOp createProcessorOp(String name, String spec, String value){
    return new IntraOp(IntraOp.Type.CREATEPROCESSOR)
            .set("name",name)
            .set("spec", spec)
            .set("value", value);
  }

 	public static IntraOp processOp(String processorName, Map params, int inputId){
 	  return new IntraOp(IntraOp.Type.PROCESS)
             .set("processorname", processorName)
             .set("params", params)
             .set("input", inputId);
 	}

 	public static IntraOp dropKeyspaceOp(String ksname){
 	  return new IntraOp(IntraOp.Type.DROPKEYSPACE)
             .set("keyspace", ksname);
 	}

  public static IntraOp createFilterOp(String name, String spec, String value) {
    return new IntraOp(IntraOp.Type.CREATEFILTER)
            .set("name", name)
            .set("spec", spec)
            .set("value", value);
  }

  public static IntraOp filterModeOp(String name, boolean on) {
    return new IntraOp(IntraOp.Type.FILTERMODE)
            .set("name", name)
            .set("on", on);
  }

  public static IntraOp cqlQuery(String query, String version){
    return new IntraOp(IntraOp.Type.CQLQUERY)
            .set("query", query)
            .set("version", version);
  }

  public static IntraOp clear(int resultId){
    return new IntraOp(IntraOp.Type.CLEAR)
            .set("id", resultId);
  }

  public static IntraOp createMultiProcess(String name, String spec, String value){
    return new IntraOp(IntraOp.Type.CREATEMULTIPROCESS)
            .set("name", name)
            .set("spec", spec)
            .set("value", value);
  }

  public static IntraOp multiProcess(String processorName, Map params){
    return new IntraOp(IntraOp.Type.MULTIPROCESS)
            .set("name", processorName)
            .set("params", params);
  }

  public static IntraOp saveState(){
    return new IntraOp(IntraOp.Type.STATE)
            .set("mode", "save");
  }

  public static IntraOp restoreState(int id){
    return new IntraOp(IntraOp.Type.STATE)
            .set("mode", "get")
            .set("id", id);
  }


  private static void checkForBlankStr(String arg, String msg, IntraOp.Type type) {
    Preconditions.checkArgument(arg != null && arg.length() > 0,
            "A non-blank '{}' is required for {}", new Object[]{msg, type});
  }
}
