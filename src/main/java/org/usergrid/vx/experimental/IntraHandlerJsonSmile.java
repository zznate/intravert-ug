package org.usergrid.vx.experimental;

import java.io.IOException;

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;
/**
 * <img src="http://www.michaeljackson.com/sites/mjackson/files/photos/Michael-Jackson-Smile-Sunglasses.jpg">
 * @author edward
 *
 */
public class IntraHandlerJsonSmile implements Handler<HttpServerRequest>{
	static IntraService is = new IntraService();
	static SmileFactory sf = new SmileFactory();
	static ObjectMapper mapper = new ObjectMapper(sf);

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
				
				is.handleIntraReq(req,res);
				
				byte [] value=null;
				try {
					value = mapper.writeValueAsBytes(value);
					System.out.println(value);
				} catch (JsonGenerationException e1) {
					e1.printStackTrace();
				} catch (JsonMappingException e1) {
					e1.printStackTrace();
				} catch (IOException e1) {
					e1.printStackTrace();
				}
				request.response.end( new Buffer(value));
			}
		});
	}
}
