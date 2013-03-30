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
package org.usergrid.vx.client;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.IntraRes;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

@Deprecated
public class IntraClient implements Handler<HttpClientResponse> {
	@SuppressWarnings("unused")
  private static Logger logger = LoggerFactory.getLogger(IntraClient.class);
	private static Vertx vertx;
	private String payload;
	private HttpClient httpClient;
	static ObjectMapper mapper = new ObjectMapper();
	ArrayBlockingQueue<IntraRes> q = new ArrayBlockingQueue<IntraRes>(1);

	public IntraClient() {
		vertx = Vertx.newVertx();
		this.httpClient = vertx.createHttpClient().setHost("localhost")
				.setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
	}

	public IntraRes sendBlocking(IntraReq i) throws Exception {
		HttpClientRequest req = httpClient.request("POST", "/intravert/intrareq-"+payload, this);
		if (payload.equalsIgnoreCase("xml")){
			ByteArrayOutputStream bo = new ByteArrayOutputStream();
			XMLEncoder e = new XMLEncoder(bo);
			e.writeObject(i);
			e.close();
			String payload = new String(bo.toByteArray());
			req.putHeader("content-length", payload.length());
			req.write(payload);
		} else if (payload.equalsIgnoreCase("json")){
			String value = mapper.writeValueAsString(i);
			req.putHeader("content-length", value.length());
			req.write(value);
		} else if (payload.equalsIgnoreCase("jsonsmile")){
		  mapper = new ObjectMapper(new SmileFactory());
		  byte [] payload =mapper.writeValueAsBytes(i);
		  req.putHeader("content-length", payload.length);
		  req.write( new Buffer(payload));
		}
    	req.exceptionHandler(new Handler<Exception>(){
            public void handle(Exception arg0){
            	System.out.println (arg0);
            }
    	});
    	req.end();
    	return q.poll(10, TimeUnit.SECONDS);
	}
	
	@Override
	public void handle(HttpClientResponse resp) {

		resp.dataHandler(new Handler<Buffer>(){
			@Override
			public void handle(Buffer arg0) {
				IntraRes ir = null;
				if (payload.equalsIgnoreCase("XML")){
					ByteArrayInputStream bi = new ByteArrayInputStream(arg0.getBytes());
					XMLDecoder d = new XMLDecoder(bi);
					ir = (IntraRes) d.readObject();
					d.close();
				} else if (payload.equalsIgnoreCase("JSON")){
					try {
						ir = mapper.readValue(arg0.getBytes(), IntraRes.class);
					} catch (JsonParseException e) {
						e.printStackTrace();
					} catch (JsonMappingException e) {
						e.printStackTrace();
					} catch (IOException e) {
						e.printStackTrace();
					}
				} else if (payload.equalsIgnoreCase("JSONSMILE")){
				  try {
            ir = mapper.readValue(arg0.getBytes(), IntraRes.class);
          } catch (JsonParseException e) {
            e.printStackTrace();
          } catch (JsonMappingException e) {
            e.printStackTrace();
          } catch (IOException e) {
            e.printStackTrace();
          }
				}
				q.add(ir);
			}
		});
	}

	
  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }
}
