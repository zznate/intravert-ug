/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.experimental;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.db.ConsistencyLevel;
import org.usergrid.vx.experimental.scan.ScanContext;
import org.usergrid.vx.experimental.scan.ScanFilter;

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
	static Map<Integer,IntraReq> preparedStatements = new HashMap<Integer,IntraReq>();
	private static AtomicInteger preparedStatementId = new AtomicInteger(0);
	Map bindParams;
	static Map<String,ScanFilter> scanFilters = new HashMap<String,ScanFilter>();
	static Map<Integer,ScanContext> openedScanners = new HashMap<Integer,ScanContext>();
	private static AtomicInteger scannerId=new AtomicInteger(0);
	
	public int saveState(IntraState s){
	  int i = id.getAndIncrement();
	  savedState.put(i,s);
	  return i;
	}
	
	public IntraState getState(int i){
	  return this.savedState.get(i);
	}
	
	public int prepareStatement(IntraReq q){
		int i = preparedStatementId.getAndIncrement();
		preparedStatements.put(i, q);
		return i;
	}

	public int openScanner(ScanContext context) {
		int id = scannerId.getAndIncrement();
		openedScanners.put(id, context);
		return id;
	}
}
