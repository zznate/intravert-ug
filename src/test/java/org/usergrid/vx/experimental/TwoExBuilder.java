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

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.usergrid.vx.server.operations.HandlerUtils;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.EventBus;
import org.vertx.java.core.json.JsonObject;

public class TwoExBuilder implements ServiceProcessor {

  /*
	@Override
	public void process(IntraReq req, IntraRes res, IntraState state, int i,
			Vertx vertx, IntraService is) {
		IntraOp op = req.getE().get(i);
		
		Map params = (Map) op.getOp().get("params");
		String uid = (String) params.get("userid");
		String fname = (String) params.get("fname");
		String lname = (String) params.get("lname");
		String city = (String) params.get("city");
		
		IntraReq innerReq = new IntraReq();
		innerReq.add( Operations.setColumnFamilyOp("users") );
		innerReq.add( Operations.setOp(uid, "fname", fname) );
		innerReq.add( Operations.setOp(uid, "lname", lname) );
		innerReq.add( Operations.setColumnFamilyOp("usersbycity") );
		innerReq.add( Operations.setOp(city, uid, "") );
		innerReq.add( Operations.setColumnFamilyOp("usersbylast") );
		innerReq.add( Operations.setOp(lname, uid, "") );
		
		IntraRes innerRes = new IntraRes();
		is.executeReq(innerReq, innerRes, state, vertx);
		
		if (innerRes.getException() != null){
			res.setExceptionAndId(innerRes.getException(), i);
		} else {
			res.getOpsRes().put(i, "OK");
		}
	}
  */
  @Override
  public void process(JsonObject request, JsonObject state, JsonObject response, EventBus eb) {
    System.out.println("called");
   
    JsonObject params = request.getObject("mpparams");
    String uid = (String) params.getString("userid");
    String fname = (String) params.getString("fname");
    String lname = (String) params.getString("lname");
    String city = (String) params.getString("city");

    RowMutation rm = new RowMutation("myks", IntraService.byteBufferForObject(uid));
    QueryPath qp = new QueryPath("users", null, IntraService.byteBufferForObject("fname"));
    rm.add(qp, IntraService.byteBufferForObject(fname), System.nanoTime());
    QueryPath qp2 = new QueryPath("users", null, IntraService.byteBufferForObject("lname"));
    rm.add(qp2, IntraService.byteBufferForObject(lname), System.nanoTime());
    
    
    RowMutation rm2 = new RowMutation("myks", IntraService.byteBufferForObject(city));
    QueryPath qp3 = new QueryPath("usersbycity", null, IntraService.byteBufferForObject(uid));
    rm2.add(qp3, IntraService.byteBufferForObject(""), System.nanoTime());
    
    QueryPath qp4 = new QueryPath("usersbylast", null, IntraService.byteBufferForObject(lname));
    rm.add(qp4, IntraService.byteBufferForObject(uid), System.nanoTime());
    List<IMutation> mutations = new ArrayList<IMutation>();
    mutations.add(rm);
    mutations.add(rm2);
    try {
      StorageProxy.mutate(mutations, ConsistencyLevel.ONE);
    } catch (WriteTimeoutException | UnavailableException | OverloadedException e) {
      e.printStackTrace();
    }

    System.out.println("done");
  }
}
