package org.usergrid.vx.experimental;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

public class IntraOp implements Serializable{

	public static final String COLUMN= "COLUMN";
	public static final String VALUE="VALUE";
	private static final AtomicInteger opid= new AtomicInteger();
	private static final long serialVersionUID = 4700563595579293663L;
	private int id;
	private String type;
	private Map<String,Object> op;

	public IntraOp(){
		op = new TreeMap<String,Object>();
		id = opid.addAndGet(1);
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
		IntraOp i = new IntraOp();
		i.setType("setkeyspace");
		i.set("keyspace", keyspace);
		return i;
	}
	
	public static IntraOp setColumnFamilyOp(String columnFamily){
		IntraOp i = new IntraOp();
		i.setType("setcolumnfamily");
		i.set("columnfamily", columnFamily);
		return i;
	}
	
	public static IntraOp setAutotimestampOp(){
		IntraOp i = new IntraOp();
		i.setType("autotimestamp");
		return i;
	}
	
	public static IntraOp setOp(Object rowkey, Object columnName, Object columnValue){
		IntraOp i = new IntraOp();
		i.setType("set");
		i.set("rowkey", rowkey);
		i.set("columnName", columnName );
		i.set("value", columnValue);
		return i;
	}
	
	public static IntraOp getOp(Object rowkey, Object columnName){
		IntraOp i = new IntraOp();
		i.setType("get");
		i.set("rowkey", rowkey);
		i.set("columnName", columnName );
		return i;
	}
	
	public static IntraOp getResRefOp(int reference, String wanted){
		IntraOp i = new IntraOp();
		i.setType("getref");
		i.set("resultref", reference);
		i.set("wanted", wanted);
		return i;
	}
	
	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}
	
}
