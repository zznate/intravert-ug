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
import org.codehaus.jackson.smile.SmileFactory;
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
	private ObjectMapper mapper ;
	private SmileFactory sf = new SmileFactory();
	private static String METHOD="POST";
	private static String ENDPOINT_JSON="/:appid/intrareq-json";
	private static String ENDPOINT_SMILE="/:appid/intrareq-smile";
	private String endpoint;
	private static final String CONTENT_LENGTH="content-length";
	public enum Transport { JSON, SMILE, XML }
	private Transport transport;
	
	public IntraClient2(String host,int port){
		vertx = Vertx.newVertx();
		httpClient = vertx.createHttpClient().setHost("localhost")
				.setPort(8080).setMaxPoolSize(10).setKeepAlive(true);
		setTransport(Transport.JSON);
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
		final AtomicReference<IntraRes> ref = new AtomicReference<IntraRes>();
		HttpClientRequest req = httpClient.request(METHOD,
				endpoint, new Handler<HttpClientResponse>() {

					@Override
					public void handle(HttpClientResponse resp) {
						resp.dataHandler(new Handler<Buffer>() {
							@Override
							public void handle(Buffer arg0) {
								IntraRes ir = null;
								try {
									ir = mapper.readValue(arg0.getBytes(), IntraRes.class);
								} catch (IOException e) {
									//TODO how do we signal exception
									//countdown on failed as well
									//e.printStackTrace();
								}
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
		
		req.putHeader(CONTENT_LENGTH, outRequest.length());
		req.end(outRequest);
		doneSignal.await();
        return ref.get();
	}

	public Transport getTransport() {
		return transport;
	}

	public void setTransport(Transport transport) {
		this.transport = transport;
		if (transport == Transport.SMILE){
			mapper = new ObjectMapper(sf);
			endpoint = ENDPOINT_SMILE;
		}
		if (transport == Transport.JSON){
			mapper = new ObjectMapper();
			endpoint = ENDPOINT_JSON;
		}
	}
	
}
