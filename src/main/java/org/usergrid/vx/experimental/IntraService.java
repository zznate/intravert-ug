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

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.CounterColumn;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.vertx.java.core.Vertx;

public class IntraService {

	//TODO: Static + bounded size?
	ExecutorService ex = Executors.newCachedThreadPool();
	public IntraRes handleIntraReq(IntraReq req, IntraRes res, Vertx vertx){
		IntraState state = new IntraState();
		if ( verifyReq(req,res) == false ){
			return res;
		} else {
			executeReq(req, res, state, vertx);
		}		
		return res;
	}
	/* Process a request, return a response.  Trap errors
	 * and set the res object appropriately. Try not to do much heavy lifting here
	 * delegate complex processing to methods.*/
	protected boolean executeReq(final IntraReq req, final IntraRes res, final IntraState state, final Vertx vertx) {
		for (int i=0;i<req.getE().size() && res.getException() == null ;i++){
			final IntraOp op = req.getE().get(i);
			final IntraService me = this;
			final int iCopy=i;
			long defaultTimeout=10000;
			
			try {
				if (op.getOp().containsKey("timeout")){
					defaultTimeout=(Integer) op.getOp().get("timeout");
				}
				Callable<Boolean> c = new Callable<Boolean>(){
					@Override
					public Boolean call() throws Exception {
						op.getType().execute(req, res, state, iCopy, vertx, me);
						//throw new RuntimeException("this is bad");
						return true;
					}
				};
				Future<Boolean> f = ex.submit(c);
				f.get(defaultTimeout, TimeUnit.MILLISECONDS);
			} catch (Exception ex){ 
			  res.setExceptionAndId(ex,i);
			  ex.printStackTrace();
			}
		}
		return true;
	}
	

  static Object resolveObject(Object o, IntraReq req, IntraRes res,IntraState state, int i){
		if (o instanceof Object[]){
		  return o;
		} else if (o instanceof Integer){
		  return o;
		} else if (o instanceof String){
			return o;
		} else if (o instanceof Map) {
                    Map<String, Object> map = (Map<String, Object>) o;
                    Object typeAttr = map.get("type");
                    if (isGetRef(typeAttr)) {
                        Map<String, Object> op = (Map<String, Object>) map.get("op");
                        Integer resultRef = (Integer) op.get("resultref");
                        String wanted = (String) op.get("wanted");
                        List referencedResult = (List) res.getOpsRes().get(resultRef);
                        Map result = (Map) referencedResult.get(0);
                        return result.get(wanted);
                    }  else if ( isBind(typeAttr) ){
                    	Integer mark = (Integer) map.get("mark");
                    	return state.bindParams.get(mark);
                    } else {
                        throw new IllegalArgumentException("Do not know what to do with " + o);
                    }
                } else if (o instanceof IntraOp){
                    IntraOp op = (IntraOp) o;
                    throw new RuntimeException(" do not know what to do with "+op.getType());
		} else {
			throw new RuntimeException(" do not know what to do with "+o.getClass());
		}
	}

    private static boolean isGetRef(Object typeAttr) {
        return typeAttr != null && typeAttr instanceof String && typeAttr.equals("GETREF");
    }
    
	private static boolean isBind(Object typeAttr) {
		return typeAttr != null && typeAttr instanceof String && typeAttr.equals("BINDMARKER");
	}


    static ByteBuffer byteBufferForObject(Object o){
	  if (o instanceof Object[]){
	    Object [] comp = (Object[]) o;
	    List<byte[]> b = new ArrayList<byte[]>();
	    int [] sep = new int [comp.length/2];
	    for (int i=0;i<comp.length;i=i+2){
	      //get the element
	      ByteBuffer element = byteBufferForObject(comp[i]);
	      byte[] by = new byte[element.remaining()];
	      element.get(by);
	      b.add(by);
	      //this part must be an unsigned int
	      sep[i/2]= (Integer) comp[i+1];
	    }
	    byte [] entireComp = CompositeTool.makeComposite(b, sep);
	    return ByteBuffer.wrap(entireComp);
	  } else if (o instanceof Integer){
	    return Int32Type.instance.decompose( (Integer)o);
	    //return ByteBufferUtil.bytes( ((Integer) o).intValue());
	  } else if (o instanceof String){
			return ByteBufferUtil.bytes((String) o);
		} else if (o instanceof byte[] ) { 
			return ByteBuffer.wrap((byte [])o);
		} else if (o instanceof ByteBuffer){
			return (ByteBuffer) o;
		}
		else throw new RuntimeException( "can not serializer "+o);
	}
	/*
	 * This is a top level sanity check. Make sure request have manditory parts. 
	 * If the IntraReq fails verifyReq it will not be processed at all. 
	 */
	public boolean verifyReq(IntraReq req, IntraRes res){
		if (req == null){
			res.setException("FATAL: REQUEST IS NULL");
			return false;
		}
		for (int i =0;i<req.getE().size();i++){
			IntraOp op = req.getE().get(i);
			if (op.getType()==null){
				res.setException("FATAL: op i had no type");
			} else if (op.getType().equals("setkeyspace")){
				
			} else if (op.getType().equals("setcolumnfamily")){
				
			} else if (op.getType().equals("autotimestamp")){
				
			} else if (op.getType().equals("set")){
			}
		}
		return true;
		
	}
	



   
  static void readCf(ColumnFamily cf , List<Map> finalResults, IntraState state, IntraOp op, NonAtomicReference lastColumn){
    Iterator<IColumn> it = cf.iterator();
    while (it.hasNext()) {
      IColumn ic = it.next();
      if (ic.isLive()){
	      HashMap m = new HashMap(5);
	      if (state.components.contains("name")){
	    	  m.put("name", TypeHelper.getTypedIfPossible(state, "column", ic.name(), op));
	      }
	      if (state.components.contains("value")){
          if ( ic instanceof CounterColumn ) {
            m.put("value", ((CounterColumn)ic).total());
          } else {
            m.put("value", TypeHelper.getTypedIfPossible(state, "value", ic.value(), op));
          }
	      }
	      if (state.components.contains("timestamp")){
	    	  m.put("timestamp",ic.timestamp());
	      }
	      if (state.components.contains("markeddelete")){
	    	  m.put("markeddelete", ic.getMarkedForDeleteAt());
	      }
              if (state.currentFilter != null) {
                  Map newMap = (Map) state.currentFilter.filter(m);
                  if (newMap != null) {
                      finalResults.add(newMap);
                  }
              } else {
                  finalResults.add(m);
              }
              lastColumn.something = m.get("name");
      }
    }
  }

	static String determineKs(Map row, IntraOp op, IntraState state) {
      String ks = null;
      if (row != null && row.containsKey("keyspace")){
    	ks = (String) row.get("keyspace");
      } else if (op.getOp().containsKey("keyspace")) {
	    ks = (String) op.getOp().get("keyspace");
	  } else {
	    ks = state.currentKeyspace;
	  }
	  return ks;
	}

	static String determineCf(Map row, IntraOp op, IntraState state) {
      String cf = null;
      if (row != null && row.containsKey("columnfamily")){
    	  cf = (String) row.get("columnfamily");
      } else if (op.getOp().containsKey("columnfamily")) {
	    cf = (String) op.getOp().get("columnfamily");
	  } else {
	    cf = state.currentColumnFamily;
	  }
	  return cf;
	}
}
