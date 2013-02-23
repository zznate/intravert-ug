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

import java.io.IOException;
import java.io.Serializable;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.*;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.RequestExecutionException;
import org.apache.cassandra.exceptions.RequestValidationException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.ClientState;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.QueryState;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ColumnPath;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.CqlRow;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.transport.messages.ResultMessage.Kind;
import org.usergrid.vx.experimental.filter.FactoryProvider;
import org.usergrid.vx.experimental.filter.FilterFactory;
import org.usergrid.vx.experimental.scan.ScanFilter;
import org.vertx.java.core.Vertx;

import groovy.lang.GroovyClassLoader;

public class IntraOp implements Serializable{

	public static final String COLUMN= "COLUMN";
	public static final String VALUE="VALUE";
  // TODO add back access to ID
	private final AtomicInteger opid= new AtomicInteger(0);
	private static final long serialVersionUID = 4700563595579293663L;
	private Type type;
	private Map<String,Object> op;

  protected IntraOp() {}
  	public String toString(){
  		return this.type + " "+this.op;
  	}
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

	public Type getType() {
		return type;
	}


  public enum Type {
    LISTCOLUMNFAMILY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        String keyspace = (String) op.getOp().get("keyspace");
        KSMetaData ks = Schema.instance.getKSMetaData(keyspace);
        res.getOpsRes().put(i, ks.cfMetaData().keySet());
      }
    },
    LISTKEYSPACES {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        res.getOpsRes().put(i, Schema.instance.getNonSystemTables());
      }
    },
    CONSISTENCY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        ConsistencyLevel level = ConsistencyLevel.valueOf((String) op.getOp().get("level"));
        res.getOpsRes().put(i, "OK");
        state.consistency = level;
      }
    },
    
    CREATEKEYSPACE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(0);
        IntraOp op = req.getE().get(i);
        String ks = (String) op.getOp().get("name");
        KsDef def = new KsDef();
        def.setName(ks);
        def.setStrategy_class("SimpleStrategy");
        Map<String, String> strat = new HashMap<String, String>();
        //TODO we should be able to get all this information from the client
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
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
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
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {

      }
    },
    SLICEBYNAMES {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
    	  IntraOp op = req.getE().get(i);
    	  List<Map> finalResults = new ArrayList<Map>();
    	  ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("rowkey"), req, res, state, i));
    	  List l = (List) op.getOp().get("wantedcols");
    	  List<ByteBuffer> columns = new ArrayList<ByteBuffer>(); 
    	  for (Object o: l){
    		  columns.add(IntraService.byteBufferForObject(IntraService.resolveObject(o, req, res, state, i)));
    	  }
    	  List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
    		ColumnParent cp = new ColumnParent();
    		cp.setColumn_family(IntraService.determineCf(null, op, state));
    		QueryPath qp = new QueryPath(cp);
    		SliceByNamesReadCommand sr= new SliceByNamesReadCommand(IntraService.determineKs(null,op,state), rowkey, cp, columns);
    		commands.add(sr);
      		List<Row> results = null;
      		NonAtomicReference lastSeen = new NonAtomicReference();
      		try {
      			results = StorageProxy.read(commands, state.consistency);
      			ColumnFamily cf = results.get(0).cf;
      			if (cf == null){ //cf= null is no data
      			} else {
      			  IntraService.readCf(cf, finalResults, state, op, lastSeen);
      			}
      			res.getOpsRes().put(i,finalResults);
      		} catch (ReadTimeoutException | org.apache.cassandra.exceptions.UnavailableException
                  | IsBootstrappingException | IOException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      			return;
      		}
      		res.getOpsRes().put(i, finalResults);
      }
    },
    SLICE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
      		List<Map> finalResults = new ArrayList<Map>();
      		ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("rowkey"), req, res, state, i));
      		ByteBuffer start = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("start"), req, res, state, i));
      		ByteBuffer end = IntraService.byteBufferForObject(IntraService.resolveObject(op.getOp().get("end"), req, res, state, i));
      		List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
      		ColumnPath cp = new ColumnPath();
      		cp.setColumn_family(IntraService.determineCf(null, op, state));
      		QueryPath qp = new QueryPath(cp);
      		SliceFromReadCommand sr = new SliceFromReadCommand(IntraService.determineKs(null, op, state), rowkey, qp, start, end, false, 100);
      		commands.add(sr);

      		List<Row> results = null;
      		NonAtomicReference lastSeen = new NonAtomicReference();
      		try {
      			results = StorageProxy.read(commands, state.consistency);
      			ColumnFamily cf = results.get(0).cf;
      			if (cf == null){ //cf= null is no data
      			} else {
      			  IntraService.readCf(cf, finalResults, state, op, lastSeen);
      			}
      		} catch (ReadTimeoutException | org.apache.cassandra.exceptions.UnavailableException
                  | IsBootstrappingException | IOException e) {
      		  res.setExceptionAndId(e.getMessage(), i);
      			return;
      		}
      		if (op.getOp().containsKey("extendedresults")){
      			Map extended = new HashMap();
      			extended.put("results", finalResults);
      			extended.put("lastname", lastSeen.something);
      			res.getOpsRes().put(i, extended);
      		} else {
      			res.getOpsRes().put(i, finalResults);
      		}
      }
    },
    COUNTER {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        String ks = IntraService.determineKs(null, op, state);
        String cf =IntraService.determineCf(null, op, state);
        RowMutation rm = new RowMutation(ks,
                IntraService.byteBufferForObject(IntraService
                        .resolveObject(op.getOp().get("rowkey"), req,
                                res, state, i)));

        rm.addCounter(new QueryPath(cf, null, IntraService.byteBufferForObject(IntraService
                                .resolveObject(op.getOp().get("name"), req,
                                        res, state, i))),
                (Long)op.getOp().get("value"));
        List<IMutation> mutations = new ArrayList(1);
        mutations.add(new CounterMutation(rm, state.consistency));
        invokeStorageProxy(mutations, state, res, i);
      }
    },
    GET {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
   
        List<Map> finalResults = new ArrayList<Map>();
        ByteBuffer rowkey = IntraService.byteBufferForObject(IntraService.resolveObject(
                op.getOp().get("rowkey"), req, res, state, i));
        ByteBuffer column = IntraService.byteBufferForObject(IntraService.resolveObject(
                op.getOp().get("name"), req, res, state, i));
        QueryPath path = new QueryPath(IntraService.determineCf(null, op, state), null);
        List<ByteBuffer> nameAsList = Arrays.asList(column);
        ReadCommand command = new SliceByNamesReadCommand(IntraService.determineKs(null, op, state),
            rowkey, path, nameAsList);
        List<Row> rows = null;

        NonAtomicReference lastSeen = new NonAtomicReference();
        try {
          rows = StorageProxy.read(Arrays.asList(command), state.consistency);
          ColumnFamily cf1 = rows.get(0).cf;
          if (cf1 == null) { // cf= null is no data
          } else {
            IntraService.readCf(cf1, finalResults, state, op , lastSeen);
          }
          res.getOpsRes().put(i, finalResults);
        } catch (ReadTimeoutException | org.apache.cassandra.exceptions.UnavailableException
                          | IsBootstrappingException | IOException e) {
          res.getOpsRes().put(i, e.getMessage());
          return;
        }
      }
    },
    SET {
      @Override
		public void execute(IntraReq req, IntraRes res, IntraState state,
				int i, Vertx vertx, IntraService is) {
			IntraOp op = req.getE().get(i);

      String ks = IntraService.determineKs(null, op, state);
      String cf =IntraService.determineCf(null, op, state);

			RowMutation rm = new RowMutation(ks,
					IntraService.byteBufferForObject(IntraService
							.resolveObject(op.getOp().get("rowkey"), req,
									res, state, i)));

			QueryPath qp = new QueryPath(cf, null,
					IntraService.byteBufferForObject(IntraService
							.resolveObject(op.getOp().get("name"), req,
									res, state, i)));
			Object val = op.getOp().get("value");
			
			if (!op.getOp().containsKey("ttl")){
				rm.add(qp, IntraService.byteBufferForObject(IntraService
					.resolveObject(val, req, res, state, i)),
					(Long) (state.autoTimestamp ? state.nanotime : op
							.getOp().get("timestamp"))); 
			} else {
				int ttl = (Integer) op.getOp().get("ttl");
				rm.add(qp, IntraService.byteBufferForObject(IntraService
						.resolveObject(val, req, res, state, i)),
						(Long) (state.autoTimestamp ? state.nanotime : op
								.getOp().get("timestamp")), ttl);				
			}
      List<IMutation> mutations = new ArrayList<IMutation>();
      mutations.add(rm);
      invokeStorageProxy(mutations, state, res, i);
		}
	},
    AUTOTIMESTAMP {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        state.autoTimestamp = true;
        res.getOpsRes().put(i, "OK");
      }
    },
    SETKEYSPACE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        state.currentKeyspace = (String) op.getOp().get("keyspace");
        res.getOpsRes().put(i, "OK");
      }
    },
    SETCOLUMNFAMILY {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        state.currentColumnFamily = (String) op.getOp().get("columnfamily");
        res.getOpsRes().put(i, "OK");
      }
    },
    ASSUME {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
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
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        GroovyClassLoader gc = new GroovyClassLoader();
        Class c = gc.parseClass((String) op.getOp().get("value") );
        Processor p = null;
        try {
          p = (Processor) c.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        IntraState.processors.put(name,p);
      }
    },
    PROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
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
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {

      }
    },
    CREATEFILTER {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
          IntraOp op = req.getE().get(i);
          String name  = (String) op.getOp().get("name");
          String scriptSource = (String) op.getOp().get("value");
          String spec = (String) op.getOp().get("spec");

          FactoryProvider factoryProvider = new FactoryProvider();
          FilterFactory filterFactory = factoryProvider.getFilterFactory(spec);
          IntraState.filters.put(name, filterFactory.createFilter(scriptSource));
      }
    },
    FILTERMODE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
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
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
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
        } catch (RequestExecutionException | RequestValidationException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        List<HashMap> returnRows = new ArrayList<HashMap>();
        if (rm.kind == Kind.ROWS) {
	      //ToDo maybe processInternal
	      CqlResult result = rm.toThriftResult();
	      //System.out.println( result.schema.name_types );
	      //System.out.println( result.schema.value_types );
	      List<CqlRow> rows = result.getRows();
	      for (CqlRow row: rows){
	        List<org.apache.cassandra.thrift.Column> columns = row.getColumns();
	        for (org.apache.cassandra.thrift.Column c: columns){
	          HashMap m = new HashMap();
	          if (op.getOp().containsKey("convert")){
	        	  m.put("name" , TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.name) );
		          m.put("value" , TypeHelper.getCqlTyped(result.schema.name_types.get(c.name), c.value) );
	          } else {
	        	  m.put("value", c.value);
	        	  m.put("name", c.name);
	          }
	          returnRows.add(m);
	        }
	      }
        }	
        res.getOpsRes().put(i,returnRows);
      }
    },
    CLEAR {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        int id = (Integer) op.getOp().get("id");
        res.getOpsRes().put(id, new ArrayList<HashMap>());
      }
    },
    CREATEMULTIPROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        String name  = (String) op.getOp().get("name");
        GroovyClassLoader gc = new GroovyClassLoader();
        Class c = gc.parseClass((String) op.getOp().get("value") );
        MultiProcessor p = null;
        try {
          p = (MultiProcessor) c.newInstance();
        } catch (InstantiationException | IllegalAccessException e) {
          res.setExceptionAndId(e, i);
          return;
        }
        IntraState.multiProcessors.put(name, p);
      }
    },
    MULTIPROCESS {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        String name = (String) op.getOp().get("name");
        Map params  = (Map) op.getOp().get("params");
        //Processor p = state.processors.get(processorName);
        MultiProcessor p = state.multiProcessors.get(name);

        List<Map> mpResults =  p.multiProcess(res.getOpsRes(), params);
        res.getOpsRes().put(i, mpResults);
      }
    },
    STATE {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i,
          Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        if (((String)op.getOp().get("mode")).equalsIgnoreCase("save")){
          res.getOpsRes().put(i, state.saveState(state));
        } else if (((String)op.getOp().get("mode")).equalsIgnoreCase("get")){
          Integer id = (Integer) op.getOp().get("id");
          IntraState other = state.getState(id.intValue());
          state.currentColumnFamily=other.currentColumnFamily;
          state.currentKeyspace=other.currentKeyspace;
          state.consistency = other.consistency;
          state.autoTimestamp = other.autoTimestamp;
          state.nanotime = other.nanotime;
          state.meta = other.meta;
          state.currentFilter = other.currentFilter;
          
        }
      }
    },
    BATCHSET {
      @Override
      public void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is) {
        IntraOp op = req.getE().get(i);
        List<Map> rows = (List<Map>) op.getOp().get("rows");
        List<IMutation> m = new ArrayList<IMutation>();
        for (Map row:rows){
          //code is mostly cloned from Type.SET
          RowMutation rm = new RowMutation(IntraService.determineKs(row, op, state),
              IntraService.byteBufferForObject(IntraService.resolveObject(
                  row.get("rowkey"), req, res, state, i)));
          QueryPath qp = new QueryPath(IntraService.determineCf(row, op , state), null,
              IntraService.byteBufferForObject(IntraService.resolveObject(
                  row.get("name"), req, res, state, i)));
          rm.add( qp,IntraService.byteBufferForObject(IntraService.resolveObject(
              row.get("value"), req, res, state, i)),
              (Long) (state.autoTimestamp ? state.nanotime : op.getOp().get(
                  "timestamp")));
          m.add(rm);
        }
        invokeStorageProxy(m, state, res,i);
      }
    }, CREATESERVICEPROCESS{
		@Override
		public void execute(IntraReq req, IntraRes res, IntraState state,
				int i, Vertx vertx, IntraService is) {
			IntraOp op = req.getE().get(i);
	        String name  = (String) op.getOp().get("name");
	        GroovyClassLoader gc = new GroovyClassLoader();
	        Class c = gc.parseClass((String) op.getOp().get("value") );
	        ServiceProcessor sp = null;
	        try {
	          sp = (ServiceProcessor) c.newInstance();
	        } catch (InstantiationException | IllegalAccessException e) {
	          res.setExceptionAndId(e, i);
	          return;
	        }
	        IntraState.serviceProcessors.put(name, sp);
	        res.getOpsRes().put(i,"OK");
		}
    }, SERVICEPROCESS {
		@Override
		public void execute(IntraReq req, IntraRes res, IntraState state,
				int i, Vertx vertx, IntraService is) {
			IntraOp op = req.getE().get(i);
			String name = (String) op.getOp().get("name");
			ServiceProcessor sp = IntraState.serviceProcessors.get(name);
			sp.process(req, res, state, i, vertx, is);
		}   	
    }, COMPONENTSELECT {
		@Override
		public void execute(IntraReq req, IntraRes res, IntraState state,
				int i, Vertx vertx, IntraService is) {
			IntraOp op = req.getE().get(i);
			Set<String> parts = (Set<String>) op.getOp().get("components");
			state.components = parts;
		}
		},
		PREPARE {
			@Override
			public void execute(IntraReq req, IntraRes res, IntraState state,
					int i, Vertx vertx, IntraService is) {
				IntraOp op = req.getE().get(i);
				req.getE().remove(i);
				int pid = state.prepareStatement(req);
				IntraReq r = new IntraReq();
				r.add(op);
				req = r;
				res.getOpsRes().put(0, pid);
			}
		},
		EXECUTEPREPARED {
			public void execute(IntraReq req, IntraRes res, IntraState state,
					int i, Vertx vertx, IntraService is) {
				IntraOp op = req.getE().get(i);
				Integer id = (Integer) op.getOp().get("pid");
				IntraReq preparedReq = IntraState.preparedStatements.get(id);
				if (preparedReq==null){
					res.setExceptionAndId( new RuntimeException("ps was null"), id);
				}
				Map bind = (Map) op.getOp().get("bind");
				state.bindParams = bind;
				IntraRes preparedRes = new IntraRes();
				is.executeReq(preparedReq, preparedRes, state, vertx);
				if (preparedRes.getException() != null) {
					res.setExceptionAndId(preparedRes.getException(), preparedRes.getExceptionId());
				}
				res.getOpsRes().putAll(preparedRes.getOpsRes());
			}
		},  CREATESCANFILTER{
			@Override
			public void execute(IntraReq req, IntraRes res, IntraState state,
					int i, Vertx vertx, IntraService is) {
				IntraOp op = req.getE().get(i);

				String name = (String) op.getOp().get("name");
				GroovyClassLoader gc = new GroovyClassLoader();
				Class c = gc.parseClass((String) op.getOp().get("value"));
				ScanFilter sf = null;
				try {
					sf = (ScanFilter) c.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					res.setExceptionAndId(e, i);
					return;
				}
				IntraState.scanFilters.put(name, sf);
				res.getOpsRes().put(i, "OK");
			}	
	    };

    public abstract void execute(IntraReq req, IntraRes res, IntraState state, int i, Vertx vertx, IntraService is);
    
    
  }

  private static void invokeStorageProxy(Collection<IMutation> mutations, IntraState state, IntraRes res, int i) {
    try {
      StorageProxy.mutate(mutations, state.consistency);
      res.getOpsRes().put(i, "OK");
    } catch (WriteTimeoutException
            | org.apache.cassandra.exceptions.UnavailableException
            | OverloadedException e) {
      res.setExceptionAndId(e.getMessage(), i);
      return;
    }
  }
}
