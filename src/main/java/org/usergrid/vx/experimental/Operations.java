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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.db.ConsistencyLevel;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Factory class for building IntraOp objects
 * @author zznate
 */
@SuppressWarnings("rawtypes")
public class Operations {

  private static final String KEYSPACE = "keyspace";
  private static final String COLUMN_FAMILY = "columnfamily";
  private static final String ROWKEY = "rowkey";
  private static final String NAME = "name";
  private static final String VALUE = "value";
  private static final String ROWS = "rows";
  private static final String START = "start";
  private static final String END = "end";
  private static final String SIZE = "size";
  private static final String WANTEDCOLS = "wantedcols";
  private static final String ACTION = "action";
  private static final String REPLICATION = "replication";
  private static final String LEVEL = "level";
  private static final String TYPE = "type";
  private static final String CLAZZ = "clazz";
  private static final String SPEC = "spec";
  private static final String PROCESSORNAME = "processorname";
  private static final String PARAMS = "params";
  private static final String INPUT = "input";
  private static final String ON = "on";
  private static final String QUERY = "query";
  private static final String VERSION = "version";
  private static final String TRANSPOSE = "transpose";
  private static final String ID = "id";
  private static final String MODE = "mode";
  private static final String SAVE = "save";
  private static final String GET = "get";

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

  public static IntraOp setAutotimestampOp(boolean on){
 		return new IntraOp(IntraOp.Type.AUTOTIMESTAMP).set("autotimestamp", on);
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

  public static IntraOp sliceOp( Object rowkey , Object start, Object end, int size){
    Preconditions.checkArgument(rowkey != null,"A row key is required for {}", IntraOp.Type.SLICE);
    Preconditions.checkArgument(size > 0, "A slice size must be positive integer for {}", IntraOp.Type.SLICE);
    return new IntraOp(IntraOp.Type.SLICE)
            .set(ROWKEY, rowkey)
            .set(START, start)
            .set(END, end)
            .set(SIZE, size);
  }

  public static IntraOp sliceByNames( Object rowkey, List columnList){
    Preconditions.checkArgument(columnList != null, "You much provide a columnList array");
 		return new IntraOp(IntraOp.Type.SLICEBYNAMES)
             .set(WANTEDCOLS, columnList).set("rowkey",rowkey);
 	}

  public static IntraOp counter( Object rowkey, Object columnName, long value) {
    Preconditions.checkArgument(rowkey != null,"A row key is required for {}", IntraOp.Type.COUNTER);
    Preconditions.checkArgument(columnName != null,"A column name is required for {}", IntraOp.Type.COUNTER);
    return new IntraOp(IntraOp.Type.COUNTER)
            .set(ROWKEY, rowkey)
            .set(NAME, columnName)
            .set(VALUE, value);
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

  public static IntraOp cqlQuery(String query, String version, boolean transpose) {
    return new IntraOp(IntraOp.Type.CQLQUERY)
        .set(QUERY, query)
        .set(VERSION, version)
        .set(TRANSPOSE, transpose);
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

	public static IntraOp createServiceProcess(String name, String spec, String value) {
		return new IntraOp(IntraOp.Type.CREATESERVICEPROCESS).set(NAME, name)
				.set(SPEC, spec).set(VALUE, value);
	}
	
	public static IntraOp serviceProcess(String name, Map params){
		return new IntraOp(IntraOp.Type.SERVICEPROCESS).set(NAME, name).set(PARAMS, params);
	}
	
	public static IntraOp componentSelect(Set<String> components){
		return new IntraOp(IntraOp.Type.COMPONENTSELECT).set("components", components);
	}
	
	public static IntraOp prepare() {
		return new IntraOp(IntraOp.Type.PREPARE);
	}

	
  @SuppressWarnings("unchecked")
  public static Map bindMarker(int mark) {
    Map m = new HashMap();
		m.put("type","BINDMARKER");
		m.put("mark", mark);
		return m;
	}
	
	public static Map ref(Integer opId, String wanted) {
		return ImmutableMap.of("type", "GETREF", "op",
				ImmutableMap.of("resultref", opId, "wanted", wanted));
	}
	
	public static IntraOp executePrepared(int pid, Map bindVars){
		IntraOp i = new IntraOp(IntraOp.Type.EXECUTEPREPARED);
		i.set("pid", pid);
		i.set("bind", bindVars);
		return i;
	}
	
	 public static IntraOp createScanFilter(String name, String spec, String value) {
		 return new IntraOp(IntraOp.Type.CREATESCANFILTER).set(NAME, name)
		 .set(SPEC, spec).set(VALUE, value);
		 }
	
  private static void checkForBlankStr(String arg, String msg, IntraOp.Type type) {
    Preconditions.checkArgument(arg != null && arg.length() > 0,
            "A non-blank '{}' is required for {}", new Object[]{msg, type});
  }
}
