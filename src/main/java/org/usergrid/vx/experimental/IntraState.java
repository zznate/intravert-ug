package org.usergrid.vx.experimental;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ConsistencyLevel;

/* class that holds properties for the request lifecycle */
public class IntraState {
	
	public IntraState(){
		components.add("name");
		components.add("value");
	}
  //TODO this should be an epiring cache
  private static Map<Integer,IntraState> savedState = new HashMap<Integer,IntraState>();
  private static AtomicInteger id=new AtomicInteger(0);
	String currentKeyspace="";
	String currentColumnFamily="";
	//TODO use enum map supposedly fast
	Set<String> components = new HashSet<String>();
	boolean autoTimestamp= true;
	long nanotime = System.nanoTime();
	ConsistencyLevel consistency= ConsistencyLevel.ONE;
	//TODO this is cookie cutter
	Map<IntraMetaData,String> meta = new HashMap<IntraMetaData,String>();
	//TODO separate per/request state from application/session state
	static Map<String,Processor> processors = new HashMap<String,Processor>();
	static Map<String,Filter> filters = new HashMap<String,Filter>(); 
	static Map<String,MultiProcessor> multiProcessors = new HashMap<String,MultiProcessor>();
	static Map<String,ServiceProcessor> serviceProcessors = new HashMap<String,ServiceProcessor>();
	Filter currentFilter;
	
	public int saveState(IntraState s){
	  int i = id.getAndIncrement();
	  savedState.put(i,s);
	  return i;
	}
	
	public IntraState getState(int i){
	  return this.savedState.get(i);
	}
}
