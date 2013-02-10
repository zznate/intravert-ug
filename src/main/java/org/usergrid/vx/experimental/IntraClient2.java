package org.usergrid.vx.experimental;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.io.Writer;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

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
		final Buffer outRequest = new Buffer();
		OutputStream st = new OutputStream(){
			@Override
			public void write(int b) throws IOException {
				outRequest.appendByte( (byte) b);
			}

			@Override
			public void write(byte[] b) throws IOException {
				outRequest.appendBytes(b);	
			}

			@Override
			public void write(byte[] b, int off, int len) throws IOException {
				byte [] b2 = new byte [len];
				System.arraycopy(b, off, b2, 0, len);
				outRequest.appendBytes(b2);
			}

			@Override
			public void flush() throws IOException {
			}

			@Override
			public void close() throws IOException {
			}
		};
		mapper.writeValue( st, i);
		final CountDownLatch doneSignal = new CountDownLatch(1);
		//final List<IntraRes> results = new LinkedList<IntraRes>();
		final AtomicReference<IntraRes> ref = new AtomicReference<IntraRes>();
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
								//results.add(ir);
								ref.set(ir);
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
		
		req.putHeader("content-length", outRequest.length());
		req.end(outRequest);
		doneSignal.await();
        return ref.get();
	}
}
