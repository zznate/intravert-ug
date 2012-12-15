package org.usergrid.vx.experimental;

import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

public class IntraHandlerXml implements Handler<HttpServerRequest>{

	static IntraService is = new IntraService();
	@Override
	public void handle(final HttpServerRequest request) {
		final IntraRes res = new IntraRes();
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
				ByteArrayInputStream i = new ByteArrayInputStream(buffer.getBytes());
				java.beans.XMLDecoder d = new java.beans.XMLDecoder(i);
				IntraReq req = (IntraReq) d.readObject();
				
				is.handleIntraReq(req,res);
				
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				XMLEncoder e = new XMLEncoder(bo);
		    	e.writeObject(res);
		    	e.close();
		    	String payload = new String(bo.toByteArray());
		    	request.response.end(payload);
			}
		});
	}
	
	
	
}
