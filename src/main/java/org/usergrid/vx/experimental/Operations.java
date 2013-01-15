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

  public static final String KEYSPACE = "keyspace";
  public static final String COLUMN_FAMILY = "columnfamily";
  public static final String ROWKEY = "rowkey";
  public static final String NAME = "name";
  public static final String VALUE = "value";
  public static final String ROWS = "rows";
  public static final String WANTED = "wanted";
  public static final String RESULTREF = "resultref";
  public static final String START = "start";
  public static final String END = "end";
  public static final String SIZE = "size";
  public static final String WANTEDCOLS = "wantedcols";
  public static final String ACTION = "action";
  public static final String REPLICATION = "replication";
  public static final String LEVEL = "level";
  public static final String TYPE = "type";
  public static final String CLAZZ = "clazz";
  public static final String SPEC = "spec";
  public static final String PROCESSORNAME = "processorname";
  public static final String PARAMS = "params";
  public static final String INPUT = "input";
  public static final String ON = "on";
  public static final String QUERY = "query";
  public static final String VERSION = "version";
  public static final String ID = "id";
  public static final String MODE = "mode";
  public static final String SAVE = "save";
  public static final String GET = "get";

  private Operations() {}

  public static IntraOp setKeyspaceOp(String keyspace){
    checkForBlankStr(keyspace, KEYSPACE, IntraOp.Type.SETKEYSPACE);
    return new IntraOp(IntraOp.Type.SETKEYSPACE)
            .set(KEYSPACE, keyspace);
  }

  public static IntraOp setColumnFamilyOp(String columnFamily){
    checkForBlankStr(columnFamily, COLUMN_FAMILY, IntraOp.Type.SETCOLUMNFAMILY);
 		return new IntraOp(IntraOp.Type.SETCOLUMNFAMILY)
             .set(COLUMN_FAMILY, columnFamily);
 	}

  public static IntraOp setAutotimestampOp(){
 		return new IntraOp(IntraOp.Type.AUTOTIMESTAMP);
 	}

  public static IntraOp setOp(Object rowkey, Object columnName, Object columnValue){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", IntraOp.Type.SET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", IntraOp.Type.SET);
    Preconditions.checkArgument(columnValue != null, "Cannot set a column to null for {}", IntraOp.Type.SET);
    return new IntraOp(IntraOp.Type.SET)
            .set(ROWKEY, rowkey)
            .set(NAME, columnName)
            .set(VALUE, columnValue);
 	}

  public static IntraOp batchSetOp(List<Map> rows){
 	  return new IntraOp(IntraOp.Type.BATCHSET)
             .set(ROWS, rows);
 	}

  public static IntraOp getOp(Object rowkey, Object columnName){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", IntraOp.Type.GET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", IntraOp.Type.GET);
 		return new IntraOp(IntraOp.Type.GET)
             .set(ROWKEY, rowkey)
             .set(NAME, columnName);
 	}

  public static IntraOp getResRefOp(int reference, String wanted){
    Preconditions.checkArgument(reference >= 0);
    checkForBlankStr(wanted, WANTED, IntraOp.Type.GETREF);
    return new IntraOp(IntraOp.Type.GETREF)
            .set(RESULTREF, reference)
            .set(WANTED, wanted);
 	}

  public static IntraOp sliceOp( Object rowkey , Object start, Object end, int size){
    Preconditions.checkArgument(rowkey != null,"A row key is required for {}", IntraOp.Type.SLICE);
    Preconditions.checkArgument(size > 0, "A slice size must be positive integer for {}", IntraOp.Type.SLICE);
    return new IntraOp(IntraOp.Type.SLICE)
            .set(ROWKEY, rowkey)
            .set(START, start)
            .set(END, end)
            .set(SIZE, size);
  }

  public static IntraOp columnPredicateOp( Object rowkey, Object [] columnList){
    Preconditions.checkArgument(columnList != null, "You much provide a columnList array");
 		return new IntraOp(IntraOp.Type.COLUMNPREDICATE)
             .set(WANTEDCOLS, columnList);
 	}

  public static IntraOp forEachOp( int opRef, IntraOp action){
    Preconditions.checkArgument(action != null, "The IntraOp action cannot be null");
    return new IntraOp(IntraOp.Type.FOREACH)
            .set(ACTION, action);
  }

 	public static IntraOp createCfOp(String cfName){
    checkForBlankStr(cfName, "columnFamily name", IntraOp.Type.CREATECOLUMNFAMILY);
 		return new IntraOp(IntraOp.Type.CREATECOLUMNFAMILY)
             .set(NAME, cfName);
 	}

  public static IntraOp createKsOp(String ksname, int replication){
    checkForBlankStr(ksname, "keyspace name", IntraOp.Type.CREATEKEYSPACE);
    Preconditions.checkArgument(replication > 0,
            "A value for positive value for 'replication' is required for {}", IntraOp.Type.CREATEKEYSPACE);
    return new IntraOp(IntraOp.Type.CREATEKEYSPACE)
            .set(NAME, ksname)
            .set(REPLICATION, replication);
  }

  public static IntraOp consistencyOp(String name){
     // Force an IllegalArgumentException
    ConsistencyLevel.valueOf(name);
 		return new IntraOp(IntraOp.Type.CONSISTENCY)
             .set(LEVEL, name);
 	}

 	public static IntraOp listKeyspacesOp(){
 	  return new IntraOp(IntraOp.Type.LISTKEYSPACES);
 	}

  public static IntraOp listColumnFamilyOp(String keyspace){
    checkForBlankStr(keyspace, "Keyspace name", IntraOp.Type.LISTCOLUMNFAMILY);
    return new IntraOp(IntraOp.Type.LISTCOLUMNFAMILY)
            .set(KEYSPACE, keyspace);
  }

  public static IntraOp assumeOp(String keyspace,String columnfamily,String type, String clazz){
    return new IntraOp(IntraOp.Type.ASSUME)
            .set(KEYSPACE, keyspace)
            .set(COLUMN_FAMILY, columnfamily)
            .set(TYPE, type) //should be column rowkey value
            .set(CLAZZ, clazz );
  }

  public static IntraOp createProcessorOp(String name, String spec, String value){
    return new IntraOp(IntraOp.Type.CREATEPROCESSOR)
            .set(NAME,name)
            .set(SPEC, spec)
            .set(VALUE, value);
  }

 	public static IntraOp processOp(String processorName, Map params, int inputId){
 	  return new IntraOp(IntraOp.Type.PROCESS)
             .set(PROCESSORNAME, processorName)
             .set(PARAMS, params)
             .set(INPUT, inputId);
 	}

 	public static IntraOp dropKeyspaceOp(String ksname){
 	  return new IntraOp(IntraOp.Type.DROPKEYSPACE)
             .set(KEYSPACE, ksname);
 	}

  public static IntraOp createFilterOp(String name, String spec, String value) {
    return new IntraOp(IntraOp.Type.CREATEFILTER)
            .set(NAME, name)
            .set(SPEC, spec)
            .set(VALUE, value);
  }

  public static IntraOp filterModeOp(String name, boolean on) {
    return new IntraOp(IntraOp.Type.FILTERMODE)
            .set(NAME, name)
            .set(ON, on);
  }

  public static IntraOp cqlQuery(String query, String version){
    return new IntraOp(IntraOp.Type.CQLQUERY)
            .set(QUERY, query)
            .set(VERSION, version);
  }

  public static IntraOp clear(int resultId){
    return new IntraOp(IntraOp.Type.CLEAR)
            .set(ID, resultId);
  }

  public static IntraOp createMultiProcess(String name, String spec, String value){
    return new IntraOp(IntraOp.Type.CREATEMULTIPROCESS)
            .set(NAME, name)
            .set(SPEC, spec)
            .set(VALUE, value);
  }

  public static IntraOp multiProcess(String processorName, Map params){
    return new IntraOp(IntraOp.Type.MULTIPROCESS)
            .set(NAME, processorName)
            .set(PARAMS, params);
  }

  public static IntraOp saveState(){
    return new IntraOp(IntraOp.Type.STATE)
            .set(MODE, SAVE);
  }

  public static IntraOp restoreState(int id){
    return new IntraOp(IntraOp.Type.STATE)
            .set(MODE, GET)
            .set(ID, id);
  }


  private static void checkForBlankStr(String arg, String msg, IntraOp.Type type) {
    Preconditions.checkArgument(arg != null && arg.length() > 0,
            "A non-blank '{}' is required for {}", new Object[]{msg, type});
  }
}
