package org.usergrid.vx.experimental;

import org.vertx.java.core.Vertx;
/* You asked for it. You are basically a first class IntraOp, and can
 * do anything. With great power comes great responsibility. */
public interface ServiceProcessor {
	public void process(IntraReq req, IntraRes res, IntraState state,
			int i, Vertx vertx, IntraService is);
}
