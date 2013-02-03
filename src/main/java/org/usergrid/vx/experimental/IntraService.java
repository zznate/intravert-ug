package org.usergrid.vx.experimental;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.vertx.java.core.Vertx;

import java.nio.ByteBuffer;
import java.util.*;

public class IntraService {


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
	protected boolean executeReq(IntraReq req, IntraRes res, IntraState state, Vertx vertx) {
		for (int i=0;i<req.getE().size() && res.getException() == null ;i++){
			IntraOp op = req.getE().get(i);
			try {
				op.getType().execute(req, res, state, i, vertx, this);
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
		} else if (o instanceof IntraOp){
			IntraOp op = (IntraOp) o;
			if (op.getType().equals(IntraOp.Type.GETREF)){
				Integer resultRef = (Integer) op.getOp().get("resultref");
				String wanted = (String) op.getOp().get("wanted");
				List aresult = (List) res.getOpsRes().get(resultRef);
				Map result = (Map) aresult.get(0);
				return result.get(wanted);
			} else {
				throw new RuntimeException(" do not know what to do with "+op.getType());
			}
		} else {
			throw new RuntimeException(" do not know what to do with "+o.getClass());
		}
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
	



   
  static void readCf(ColumnFamily cf , List<Map> finalResults, IntraState state, IntraOp op){
    Iterator<IColumn> it = cf.iterator();
    while (it.hasNext()) {
      IColumn ic = it.next();
      if (ic.isLive()){
	      HashMap m = new HashMap();
	      if (state.components.contains("name")){
	    	  m.put("name", TypeHelper.getTypedIfPossible(state, "column", ic.name(), op));
	      }
	      if (state.components.contains("value")){
	    	  m.put("value", TypeHelper.getTypedIfPossible(state, "value", ic.value(), op));
	      }
	      if (state.components.contains("timestamp")){
	    	  m.put("timestamp",ic.timestamp());
	      }
	      if (state.components.contains("markeddelete")){
	    	  m.put("markeddelete", ic.getMarkedForDeleteAt());
	      }
	      if (state.currentFilter != null){
	        Map newMap = state.currentFilter.filter(m);
	        if (newMap != null){
	          finalResults.add(newMap);
	        }
	      } else {
	        finalResults.add(m);
	      }
      }
    }
  }

	static String determineKs(IntraOp op, IntraState state) {
      String ks = null;
	  if (op.getOp().containsKey("keyspace")) {
	    ks = (String) op.getOp().get("keyspace");
	  } else {
	    ks = state.currentKeyspace;
	  }
	  return ks;
	}

	static String determineCf(IntraOp op, IntraState state) {
      String cf = null;
	  if (op.getOp().containsKey("columnfamily")) {
	    cf = (String) op.getOp().get("columnfamily");
	  } else {
	    cf = state.currentColumnFamily;
	  }
	  return cf;
	}
}
