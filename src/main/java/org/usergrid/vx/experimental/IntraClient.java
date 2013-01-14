package org.usergrid.vx.experimental;

import java.beans.XMLDecoder;
import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.Future;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;


import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

public class IntraClient implements Handler<HttpClientResponse> {
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
		HttpClientRequest req = httpClient.request("POST", "/:appid/intrareq-"+payload, this);
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

	public static void main(String[] args) throws Exception {
		IntraClient i = new IntraClient();
		i.payload="json";
		IntraReq req = new IntraReq();
		req.add( IntraOp.setKeyspaceOp("myks") );
		req.add( IntraOp.createKsOp("myks", 1));
		req.add( IntraOp.createCfOp("mycf"));
		req.add( IntraOp.setColumnFamilyOp("mycf") );
		req.add( IntraOp.setAutotimestampOp() );
		req.add( IntraOp.setOp("5", "6", "7"));
		req.add( IntraOp.sliceOp("5", "1", "9", 4));
		//req.add( IntraOp.setOp("bob",  new Object [] { 4, "stuff" }, 10) );
		//req.add( IntraOp.getOp("bob", new Object [] { 4, "stuff" }) );
		//req.add( IntraOp.setKeyspaceOp("otherks") );
		//req.add( IntraOp.setColumnFamilyOp("othercf") );
		//req.add( IntraOp.getOp( IntraOp.getResRefOp(-3, IntraOp.VALUE),  "wantedcolumn") );
		//req.add( IntraOp.sliceOp(10, "a", "g", 100) );
		//req.add( IntraOp.setColumnFamilyOp("anothercf") );
		//req.add( IntraOp.forEachOp(-2, 
		//		IntraOp.sliceOp( IntraOp.getResRefOp(-2, "COLUMN"), "a", "z", 10)
		//		)
		//);
		
		System.out.println( i.sendBlocking(req) );
		Thread.sleep(4000);
		
	}

  public String getPayload() {
    return payload;
  }

  public void setPayload(String payload) {
    this.payload = payload;
  }
}
