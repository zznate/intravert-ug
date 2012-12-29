package org.usergrid.vx.experimental;
 
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

public class IntraHandlerJson implements Handler<HttpServerRequest>{

	static IntraService is = new IntraService();
	static ObjectMapper mapper = new ObjectMapper();
	
  private Vertx vertx;
  
  public IntraHandlerJson(Vertx vertx){
    super();
    this.vertx=vertx;
  }
	
	@Override
	public void handle(final HttpServerRequest request) {
		final IntraRes res = new IntraRes();
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
			  
				IntraReq req = null;
				try {
					req = mapper.readValue(buffer.getBytes(), IntraReq.class);
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
				is.handleIntraReq(req,res,vertx);
				
				String value=null;
				try {
					value = mapper.writeValueAsString(res);
				} catch (JsonGenerationException e1) {
					e1.printStackTrace();
				} catch (JsonMappingException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				System.out.println(value);
				request.response.end(value);
			}
		});
	}
}
