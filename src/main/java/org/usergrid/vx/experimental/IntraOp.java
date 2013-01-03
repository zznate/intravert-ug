package org.usergrid.vx.experimental;

import com.google.common.base.Preconditions;
import groovy.lang.GroovyClassLoader;
import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.Column;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.*;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.*;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.vertx.java.core.Vertx;

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.*;
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

  public static IntraOp createFilterOp(String name, String spec, String value) {
    IntraOp i = new IntraOp(Type.CREATEFILTER);
    i.set("name", name);
    i.set("spec", spec);
    i.set("value", value);
    return i;
  }

  public static IntraOp filterModeOp(String name, boolean on) {
    IntraOp i = new IntraOp(Type.FILTERMODE);
    i.set("name", name);
    i.set("on", on);
    return i;
  }
  
  public static IntraOp cqlQuery(String query, String version){
    IntraOp i = new IntraOp(Type.CQLQUERY);
    i.set("query", query);
    i.set("version", version );
    return i;
  }

  public static IntraOp clear(int resultId){
    IntraOp i = new IntraOp(Type.CLEAR);
    i.set("id", resultId);
    return i;
  }
  
  public static IntraOp createMultiProcess(String name, String spec, String value){
    IntraOp i = new IntraOp(Type.CREATEMULTIPROCESS);
    i.set("name", name);
    i.set("spec", spec);
    i.set("value", value);
    return i;
  }
  
  public static IntraOp multiProcess(String processorName, Map params){
    IntraOp i = new IntraOp(Type.MULTIPROCESS);
    i.set("name", processorName);
    i.set("params", params);
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
    LISTCOLUMNFAMILY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String keyspace = (String) op.getOp().get("keyspace");
        KSMetaData ks = Schema.instance.getKSMetaData(keyspace);
        res.getOpsRes().put(i, ks.cfMetaData().keySet());
      }
    },
    LISTKEYSPACES {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        res.getOpsRes().put(i, Schema.instance.getNonSystemTables());
      }
    },
    CONSISTENCY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        ConsistencyLevel level = ConsistencyLevel.valueOf((String) op.getOp().get("level"));
        res.getOpsRes().put(i, "OK");
        state.consistency = level;
      }
    },
    CREATEKEYSPACE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(0);
        IntraOp op = req.getE().get(i);
        String ks = (String) op.getOp().get("name");
        KsDef def = new KsDef();
        def.setName(ks);
        def.setStrategy_class("SimpleStrategy");
        Map<String, String> strat = new HashMap<String, String>();
        strat.put("replication_factor", "1");
        def.setStrategy_options(strat);
        KSMetaData ksm = null;
        try {
          ksm = KSMetaData.fromThrift(def,
                  cfDefs.toArray(new CFMetaData[cfDefs.size()]));
        } catch (ConfigurationException e) {
          res.setExceptionAndId(e.getMessage(), i);
          return;
        }

        try {
          MigrationManager.announceNewKeyspace(ksm);
        } catch (ConfigurationException e) {
          res.setExceptionAndId(e.getMessage(), i);
          return;
        }
        res.getOpsRes().put(i, "OK");
      }
    },
    CREATECOLUMNFAMILY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String cf = (String) op.getOp().get("name");
        CfDef def = new CfDef();
        def.setName(cf);
        def.setKeyspace(state.currentKeyspace);
        def.unsetId();
        CFMetaData cfm = null;

        try {
          cfm = CFMetaData.fromThrift(def);
          cfm.addDefaultIndexNames();
        } catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
          res.setExceptionAndId(e.getMessage(), i);
          return;
        } catch (ConfigurationException e) {
          res.setExceptionAndId(e.getMessage(), i);
          return;
        }
        try {
          MigrationManager.announceNewColumnFamily(cfm);
        } catch (ConfigurationException e) {
          res.setExceptionAndId(e.getMessage(), i);
          return;
        }
        res.getOpsRes().put(i, "OK");
      }
    },
    FOREACH {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {

      }
    },
    COLUMNPREDICATE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {

      }
    },
    SLICE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
      		List<Map> finalResults = new ArrayList<Map>();
      		ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("rowkey"), req, res, state, i));
      		ByteBuffer start = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("start"), req, res, state, i));
      		ByteBuffer end = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("end"), req, res, state, i));
      		List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
      		ColumnPath cp = new ColumnPath();
      		cp.setColumn_family(state.currentColumnFamily);
      		QueryPath qp = new QueryPath(cp);
      		SliceFromReadCommand sr = new SliceFromReadCommand(state.currentKeyspace, rowkey, qp, start, end, false, 100);
      		commands.add(sr);

      		List<Row> results = null;
      		try {
      			results = StorageProxy.read(commands, state.consistency);
      			ColumnFamily cf = results.get(0).cf;
      			if (cf == null){ //cf= null is no data
      			} else {
      			  IntraService.readCf(cf, finalResults, state);
      			}
      			res.getOpsRes().put(i,finalResults);
      		} catch (ReadTimeoutException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      			return;
      		} catch (org.apache.cassandra.exceptions.UnavailableException | IsBootstrappingException | IOException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      			return;
      		}
      		res.getOpsRes().put(i, finalResults);
      }
    },
    GETREF {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {

      }
    },
    GET {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        List<Map> finalResults = new ArrayList<Map>();
        ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(
                op.getOp().get("rowkey"), req, res, state, i));
        ByteBuffer column = IntraService.byteBufferForObject(IntraService.resolveObject(
                op.getOp().get("column"), req, res, state, i));
        QueryPath path = new QueryPath(state.currentColumnFamily, null);
        List<ByteBuffer> nameAsList = Arrays.asList(column);
        ReadCommand command = new SliceByNamesReadCommand(state.currentKeyspace,
            rowkey, path, nameAsList);
        List<Row> rows = null;

        try {
          rows = StorageProxy.read(Arrays.asList(command), state.consistency);
          ColumnFamily cf = rows.get(0).cf;
          if (cf == null) { // cf= null is no data
          } else {
            IntraService.readCf(cf, finalResults, state);
          }
          res.getOpsRes().put(i, finalResults);
        } catch (ReadTimeoutException e) {
          res.getOpsRes().put(i, e.getMessage());
          return;
        } catch (UnavailableException e) {
          res.getOpsRes().put(i, e.getMessage());
          return;
        } catch (IsBootstrappingException e) {
          res.getOpsRes().put(i, e.getMessage());
          return;
        } catch (IOException e) {
          res.getOpsRes().put(i, e.getMessage());
          return;
        }
      }
    },
    SET {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
      		RowMutation rm = new RowMutation(state.currentKeyspace,
                  IntraService.byteBufferForObject(
                          IntraService.resolveObject(op.getOp().get("rowkey"), req, res, state, i)
                  ));
      		QueryPath qp = new QueryPath(state.currentColumnFamily,null, IntraService.byteBufferForObject(
                  IntraService.resolveObject(op.getOp().get("columnName"), req, res, state, i)
          ) );
      		Object val = op.getOp().get("value");
      		rm.add(qp, IntraService.byteBufferForObject(
                  IntraService.resolveObject(val, req, res, state, i)
          ), (Long) (state.autoTimestamp ? state.nanotime : op.getOp().get("timestamp")));
      		try {
      			StorageProxy.mutate(Arrays.asList(rm), state.consistency);
      			res.getOpsRes().put(i, "OK");
      		} catch (WriteTimeoutException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      		  return;
      		} catch (org.apache.cassandra.exceptions.UnavailableException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      		  return;
      		} catch (OverloadedException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      		  return;
      		}
      }
    },
    AUTOTIMESTAMP {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        state.autoTimestamp = true;
        res.getOpsRes().put(i, "OK");
      }
    },
    SETKEYSPACE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        state.currentKeyspace = (String) op.getOp().get("keyspace");
        res.getOpsRes().put(i, "OK");
      }
    },
    SETCOLUMNFAMILY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        state.currentColumnFamily = (String) op.getOp().get("columnfamily");
        res.getOpsRes().put(i, "OK");
      }
    },
    ASSUME {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        IntraMetaData imd = new IntraMetaData();
        imd.keyspace = (String) op.getOp().get("keyspace");
        imd.columnfamily = (String) op.getOp().get("columnfamily");
        imd.type = (String) op.getOp().get("type");
        state.meta.put( imd , (String) op.getOp().get("clazz") );
        res.getOpsRes().put(i, "OK");
      }
    },
    CREATEPROCESSOR {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        GroovyClassLoader gc = new GroovyClassLoader();
        Class c = gc.parseClass((String) op.getOp().get("value") );
        Processor p = null;
        try {
          p = (Processor) c.newInstance();
        } catch (InstantiationException e) {
          res.setExceptionAndId(e, i);
          return;
        } catch (IllegalAccessException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        IntraState.processors.put(name,p);
      }
    },
    PROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String processorName = (String) op.getOp().get("processorname");
        Map params  = (Map) op.getOp().get("params");
        Processor p = state.processors.get(processorName);
        Integer inputId = (Integer) op.getOp().get("input");
        List<Map> toProcess = (List<Map>)res.getOpsRes().get(inputId);
        List<Map> results = p.process(toProcess);
        res.getOpsRes().put(i, results);
      }
    },
    DROPKEYSPACE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {

      }
    },
    CREATEFILTER {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        GroovyClassLoader gc = new GroovyClassLoader();
        Class c = gc.parseClass((String) op.getOp().get("value") );
        Filter f = null;
        try {
          f = (Filter) c.newInstance();
        } catch (InstantiationException e) {
          res.setExceptionAndId(e, i);
          return;
        } catch (IllegalAccessException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        IntraState.filters.put(name, f);
      }
    },
    FILTERMODE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        Boolean on  = (Boolean) op.getOp().get("on");
        if (on){
          Filter f = state.filters.get(name);
          if (f == null){
            res.setExceptionAndId("filter "+name +" not found", i);
            return;
          } else {
            state.currentFilter=f;
          }
        } else {
          state.currentFilter = null;
        }
      }
    },
    CQLQUERY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        ClientState clientState = new ClientState();
        try {
          clientState.setCQLVersion((String) op.getOp().get("version"));
          clientState.setKeyspace(state.currentKeyspace);
        } catch (InvalidRequestException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        QueryState queryState = new QueryState(clientState);
        ResultMessage rm = null;
        try {
          rm = QueryProcessor.process((String) op.getOp().get("query"), state.consistency, queryState);
        } catch (RequestExecutionException e) {
          res.setExceptionAndId(e, i);
          return;
        } catch (RequestValidationException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        //ToDo maybe processInternal
        CqlResult result = rm.toThriftResult();

        List<CqlRow> rows = result.getRows();
        List<HashMap> returnRows = new ArrayList<HashMap>();
        for (CqlRow row: rows){
          List<org.apache.cassandra.thrift.Column> columns = row.getColumns();
          for (org.apache.cassandra.thrift.Column c: columns){
            HashMap m = new HashMap();
            m.put("value", c.value);
            m.put("name", c.name);
            returnRows.add(m);
          }
        }
        res.getOpsRes().put(i,returnRows);
      }
    },
    CLEAR {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        int id = (Integer) op.getOp().get("id");
        res.getOpsRes().put(id, new ArrayList<HashMap>());
      }
    },
    CREATEMULTIPROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        GroovyClassLoader gc = new GroovyClassLoader();
        Class c = gc.parseClass((String) op.getOp().get("value") );
        MultiProcessor p = null;
        try {
          p = (MultiProcessor) c.newInstance();
        } catch (InstantiationException e) {
          res.setExceptionAndId(e, i);
          return;
        } catch (IllegalAccessException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        IntraState.multiProcessors.put(name, p);
      }
    },
    MULTIPROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx) {
        IntraOp op = req.getE().get(i);
        String name = (String) op.getOp().get("name");
        Map params  = (Map) op.getOp().get("params");
        //Processor p = state.processors.get(processorName);
        MultiProcessor p = state.multiProcessors.get(name);

        List<Map> mpResults =  p.multiProcess(res.getOpsRes(), params);
        res.getOpsRes().put(i, mpResults);
      }
    };

    public abstract void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx);
    
  }
	
}
