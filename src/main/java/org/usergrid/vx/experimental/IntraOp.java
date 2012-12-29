package org.usergrid.vx.experimental;

import com.google.common.base.Preconditions;
import org.apache.cassandra.db.ConsistencyLevel;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IntraOp implements Serializable{

	public static final String COLUMN= "COLUMN";
	public static final String VALUE="VALUE";
  // TODO add back access to ID
	private final AtomicInteger opid= new AtomicInteger(0);
	private static final long serialVersionUID = 4700563595579293663L;
	private Type type;
	private Map<String,Object> op;

  protected IntraOp() {}

	IntraOp(Type type){
    this.type = type;
		op = new TreeMap<String,Object>();
	}

	public IntraOp set(String key, Object value){
		op.put(key,value);
		return this;
	}


	public Map<String, Object> getOp() {
		return op;
	}

	
	public static IntraOp setKeyspaceOp(String keyspace){
    checkForBlankStr(keyspace, "keyspace", Type.SETKEYSPACE);
		IntraOp i = new IntraOp(Type.SETKEYSPACE);
		i.set("keyspace", keyspace);
		return i;
	}

	public static IntraOp setColumnFamilyOp(String columnFamily){
    checkForBlankStr(columnFamily, "columnFamily",Type.SETCOLUMNFAMILY);
		IntraOp i = new IntraOp(Type.SETCOLUMNFAMILY);
		i.set("columnfamily", columnFamily);
		return i;
	}
	
	public static IntraOp setAutotimestampOp(){
		IntraOp i = new IntraOp(Type.AUTOTIMESTAMP);
		return i;
	}
	
	public static IntraOp setOp(Object rowkey, Object columnName, Object columnValue){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", Type.SET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", Type.SET);
    Preconditions.checkArgument(columnValue != null, "Cannot set a column to null for {}", Type.SET);
		IntraOp i = new IntraOp(Type.SET);
		i.set("rowkey", rowkey);
		i.set("columnName", columnName);
		i.set("value", columnValue);
		return i;
	}
	
	public static IntraOp getOp(Object rowkey, Object columnName){
    Preconditions.checkArgument(rowkey != null, "The rowkey cannot be null for {}", Type.GET);
    Preconditions.checkArgument(columnName != null, "The columnName cannot be null for {}", Type.GET);
		IntraOp i = new IntraOp(Type.GET);
		i.set("rowkey", rowkey);
		i.set("column", columnName);
		return i;
	}
	
	public static IntraOp getResRefOp(int reference, String wanted){
    Preconditions.checkArgument(reference >= 0);
    checkForBlankStr(wanted, "wanted", Type.GETREF);
		IntraOp i = new IntraOp(Type.GETREF);
		i.set("resultref", reference);
		i.set("wanted", wanted);
		return i;
	}
	
	public static IntraOp sliceOp( Object rowkey , Object start, Object end, int size){
    Preconditions.checkArgument(rowkey != null,"A row key is required for {}", Type.SLICE);
    Preconditions.checkArgument(size > 0, "A slice size must be positive integer for {}", Type.SLICE);
		IntraOp i = new IntraOp(Type.SLICE);
		i.set("rowkey", rowkey);
		i.set("start", start);
		i.set("end", end);
		i.set("size", size);
		return i;
	}
	
	public static IntraOp columnPredicateOp( Object rowkey, Object [] columnList){
    Preconditions.checkArgument(columnList != null, "You much provide a columnList array");
		IntraOp i = new IntraOp(Type.COLUMNPREDICATE);
		i.set("wantedcols", columnList);
		return i;
	}
	
	public static IntraOp forEachOp( int opRef, IntraOp action){
    Preconditions.checkArgument(action != null, "The IntraOp action cannot be null");
		IntraOp i = new IntraOp(Type.FOREACH);
		i.set("action", action);
		return i;
	}
	
	public static IntraOp createCfOp(String cfName){
    checkForBlankStr(cfName, "columnFamily name", Type.CREATECOLUMNFAMILY);
		IntraOp i = new IntraOp(Type.CREATECOLUMNFAMILY);
		i.set("name", cfName);
		return i;
	}
	
	public static IntraOp createKsOp(String ksname, int replication){
    checkForBlankStr(ksname, "keyspace name", Type.CREATEKEYSPACE);
    Preconditions.checkArgument(replication > 0,
            "A value for positive value for 'replication' is required for {}", Type.CREATEKEYSPACE);
		IntraOp i = new IntraOp(Type.CREATEKEYSPACE);
		i.set("name", ksname);
		i.set("replication", replication);
		return i;
	}
	
	public static IntraOp consistencyOp(String name){
    // Force an IllegalArgumentException
    ConsistencyLevel.valueOf(name);
		IntraOp i = new IntraOp(Type.CONSISTENCY);
		i.set("level", name);
		return i;
	}
	
	public static IntraOp listKeyspacesOp(){
	  IntraOp i = new IntraOp(Type.LISTKEYSPACES);
    return i;
	}
	
	public static IntraOp listColumnFamilyOp(String keyspace){
    checkForBlankStr(keyspace, "Keyspace name", Type.LISTCOLUMNFAMILY);
    IntraOp i = new IntraOp(Type.LISTCOLUMNFAMILY);
    i.set("keyspace", keyspace);
    return i;
	}
	
	public static IntraOp assumeOp(String keyspace,String columnfamily,String type, String clazz){
	  IntraOp i = new IntraOp(Type.ASSUME);
	  i.set("keyspace", keyspace);
	  i.set("columnfamily", columnfamily);
	  i.set("type", type); //should be column rowkey value
	  i.set("clazz", clazz );
	  return i;
	}
	
	public static IntraOp createProcessorOp(String name, String spec, String value){
	  IntraOp i = new IntraOp(Type.CREATEPROCESSOR);
	  i.set("name",name);
	  i.set("spec", spec);
	  i.set("value", value);
	  return i;
	}
	
	public static IntraOp processOp(String processorName, Map params, int inputId){
	  IntraOp i = new IntraOp(Type.PROCESS);
	  i.set("processorname", processorName);
	  i.set("params", params);
	  i.set("input", inputId);
	  return i;
	}
	
	public static IntraOp dropKeyspaceOp(String ksname){
	  IntraOp i = new IntraOp(Type.DROPKEYSPACE);
	  i.set("keyspace", ksname);
	  return i;
	}
	
	public Type getType() {
		return type;
	}

  private static void checkForBlankStr(String arg, String msg, Type type) {
    Preconditions.checkArgument(arg != null && arg.length() > 0,
                "A non-blank '{}' is required for {}", new Object[]{msg,type});
  }

  public enum Type {
    LISTCOLUMNFAMILY,
    LISTKEYSPACES,
    CONSISTENCY,
    CREATEKEYSPACE,
    CREATECOLUMNFAMILY,
    FOREACH,
    COLUMNPREDICATE,
    SLICE,
    GETREF,
    GET,
    SET,
    AUTOTIMESTAMP,
    SETKEYSPACE,
    SETCOLUMNFAMILY,
    ASSUME,
    CREATEPROCESSOR,
    PROCESS,
    DROPKEYSPACE
  }
	
}
