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

import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

public class IntraHandlerXml implements Handler<HttpServerRequest>{
  
	static IntraService is = new IntraService();
  private Vertx vertx;
  
  public IntraHandlerXml(Vertx x){
    vertx=x;
  }
  
	@Override
	public void handle(final HttpServerRequest request) {
		final IntraRes res = new IntraRes();
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
				ByteArrayInputStream i = new ByteArrayInputStream(buffer.getBytes());
				java.beans.XMLDecoder d = new java.beans.XMLDecoder(i);
				IntraReq req = (IntraReq) d.readObject();
				
				is.handleIntraReq(req,res,vertx);
				
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
