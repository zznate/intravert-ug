package org.usergrid.vx.experimental;

import java.util.Map;
import org.vertx.java.core.Vertx;

public class TwoExBuilder implements ServiceProcessor {

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

}
