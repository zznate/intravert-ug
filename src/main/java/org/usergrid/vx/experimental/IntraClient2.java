package org.usergrid.vx.experimental;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;

public class IntraClient2 {
	private static Logger logger = LoggerFactory.getLogger(IntraClient.class);
	private Vertx vertx;
	private HttpClient httpClient;
	static ObjectMapper mapper = new ObjectMapper();
	private static String METHOD="POST";
	private static String ENDPOINT="/:appid/intrareq-json";
	
	public IntraClient2(String host,int port){
		vertx = Vertx.newVertx();
		this.httpClient = vertx.createHttpClient().setHost("localhost")
				.setPort(8080).setMaxPoolSize(10).setKeepAlive(true);
	}
	
	public IntraRes sendBlocking(IntraReq i) throws Exception {
		//String value = mapper.writeValueAsString(i);
		ByteArrayOutputStream out = new ByteArrayOutputStream();
		mapper.writeValue(out, i);
		final CountDownLatch doneSignal = new CountDownLatch(1);
		final List<IntraRes> results = new LinkedList<IntraRes>();
		HttpClientRequest req = httpClient.request(METHOD,
				ENDPOINT, new Handler<HttpClientResponse>() {

					@Override
					public void handle(HttpClientResponse resp) {
						resp.dataHandler(new Handler<Buffer>() {
							@Override
							public void handle(Buffer arg0) {
								IntraRes ir = null;
								try {
									ir = mapper.readValue(arg0.getBytes(), IntraRes.class);
								} catch (IOException e) {
									// TODO Auto-generated catch block
									e.printStackTrace();
								}
								results.add(ir);
							}
						});

						resp.endHandler(new SimpleHandler() {
							@Override
							protected void handle() {
								doneSignal.countDown();
							}
						});

					}
				});
		req.putHeader("content-length", out.size());
		Buffer b = new Buffer(out.toByteArray());
		req.end(b);
		
        doneSignal.await();
        return results.get(0);
	}
}
