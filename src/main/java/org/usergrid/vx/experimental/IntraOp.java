package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IntraOp implements Serializable{

	public static final String COLUMN= "COLUMN";
	public static final String VALUE="VALUE";
	private final AtomicInteger opid= new AtomicInteger(0);
	private static final long serialVersionUID = 4700563595579293663L;
	private int id;
	private final Type type;
	private Map<String,Object> op;

	IntraOp(Type type){
    this.type = type;
		op = new TreeMap<String,Object>();
		id = opid.getAndAdd(1);
	}

	public IntraOp set(String key, Object value){
		op.put(key,value);
		return this;
	}

	public int getId() {
		return id;
	}

	public void setId(int id) {
		this.id = id;
	}

	public Map<String, Object> getOp() {
		return op;
	}

	public void setOp(Map<String, Object> op) {
		this.op = op;
	}
	
	public static IntraOp setKeyspaceOp(String keyspace){
		IntraOp i = new IntraOp(Type.SETKEYSPACE);
		i.set("keyspace", keyspace);
		return i;
	}
	
	public static IntraOp setColumnFamilyOp(String columnFamily){
		IntraOp i = new IntraOp(Type.SETCOLUMNFAMILY);
		i.set("columnfamily", columnFamily);
		return i;
	}
	
	public static IntraOp setAutotimestampOp(){
		IntraOp i = new IntraOp(Type.AUTOTIMESTAMP);
		return i;
	}
	
	public static IntraOp setOp(Object rowkey, Object columnName, Object columnValue){
		IntraOp i = new IntraOp(Type.SET);
		i.set("rowkey", rowkey);
		i.set("columnName", columnName);
		i.set("value", columnValue);
		return i;
	}
	
	public static IntraOp getOp(Object rowkey, Object columnName){
		IntraOp i = new IntraOp(Type.GET);
		i.set("rowkey", rowkey);
		i.set("column", columnName);
		return i;
	}
	
	public static IntraOp getResRefOp(int reference, String wanted){
		IntraOp i = new IntraOp(Type.GETREF);
		i.set("resultref", reference);
		i.set("wanted", wanted);
		return i;
	}
	
	public static IntraOp sliceOp( Object rowkey , Object start, Object end, int size){
		IntraOp i = new IntraOp(Type.SLICE);
		i.set("rowkey", rowkey);
		i.set("start", start);
		i.set("end", end);
		i.set("size", size);
		return i;
	}
	
	public static IntraOp columnPredicateOp( Object rowkey, Object [] columnList){
		IntraOp i = new IntraOp(Type.COLUMNPREDICATE);
		i.set("wantedcols", columnList);
		return i;
	}
	
	public static IntraOp forEachOp( int opRef, IntraOp action){
		IntraOp i = new IntraOp(Type.FOREACH);
		i.set("action", action);
		return i;
	}
	
	public static IntraOp createCfOp(String cfName){
		IntraOp i = new IntraOp(Type.CREATECOLUMNFAMILY);
		i.set("name", cfName);
		return i;
	}
	
	public static IntraOp createKsOp(String ksname, int replication){
		IntraOp i = new IntraOp(Type.CREATEKEYSPACE);
		i.set("name", ksname);
		i.set("replication", replication);
		return i;
	}
	
	public static IntraOp consistencyOp(String name){
		IntraOp i = new IntraOp(Type.CONSISTENCY);
		i.set("level", name);
		return i;
	}
	
	public static IntraOp listKeyspacesOp(){
	  IntraOp i = new IntraOp(Type.LISTKEYSPACES);
    return i;
	}
	
	public static IntraOp listColumnFamilyOp(String keyspace){
	  IntraOp i = new IntraOp(Type.LISTCOLUMNFAMILY);
    i.set("keyspace", keyspace);
    return i;
	}
	
	public Type getType() {
		return type;
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
    SETCOLUMNFAMILY
  }
	
}
