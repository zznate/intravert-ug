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

import java.util.Map;

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
    response.putString("a", "OK");
    //operation.putObject("mpparams", theParams);
    //operation.putObject("mpres", results.getObject("opsRes"));
   
    JsonObject params = request.getObject("mpparams");
    String uid = (String) params.getString("userid");
    String fname = (String) params.getString("fname");
    String lname = (String) params.getString("lname");
    String city = (String) params.getString("city");

    IntraReq innerReq = new IntraReq();
    innerReq.add(Operations.setKeyspaceOp("myks"));
    innerReq.add(Operations.setColumnFamilyOp("users"));
    innerReq.add(Operations.setAutotimestampOp(true));
    // innerReq.add( Operations.setOp(uid, "fname", fname) );
    // innerReq.add( Operations.setOp(uid, "lname", lname) );
    /*
     * innerReq.add( Operations.setColumnFamilyOp("usersbycity") ); innerReq.add(
     * Operations.setOp(city, uid, "") ); innerReq.add( Operations.setColumnFamilyOp("usersbylast")
     * ); innerReq.add( Operations.setOp(lname, uid, "") );
     */
    System.out.println("Sending internal request");
    IntraRes res = HandlerUtils.handleRequestBlocking(innerReq, eb);

    System.out.println("The inner result is " + res);
  }
}
