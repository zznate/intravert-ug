package org.usergrid.vx.client;

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
import org.vertx.java.platform.PlatformLocator;

import java.io.IOException;
import java.io.OutputStream;
import java.util.concurrent.CountDownLatch;

public class IntraClient2 {
	@SuppressWarnings("unused")
  private static Logger logger = LoggerFactory.getLogger(IntraClient2.class);
	private Vertx vertx;
	private HttpClient httpClient;
	private ObjectMapper mapper ;
	private SmileFactory sf = new SmileFactory();
	private static String METHOD="POST";
	private static String ENDPOINT_JSON="/intravert/intrareq-json";
	private static String ENDPOINT_SMILE="/intravert/intrareq-smile";
	private String endpoint;
	private static final String CONTENT_LENGTH="content-length";
	public enum Transport { JSON, SMILE, XML }
	private Transport transport;
	
	public IntraClient2(String host,int port){
		vertx = PlatformLocator.factory.createPlatformManager().vertx();
		httpClient = vertx.createHttpClient().setHost(host)
				.setPort(port).setMaxPoolSize(10).setKeepAlive(true);
		setTransport(Transport.JSON);
	}
	
	public IntraRes sendBlocking(IntraReq i) throws Exception {
    final Buffer buffer = new Buffer();
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

		HttpClientRequest req = httpClient.request(METHOD,
				endpoint, new Handler<HttpClientResponse>() {

					@Override
					public void handle(HttpClientResponse resp) {
						resp.dataHandler(new Handler<Buffer>() {
							@Override
							public void handle(Buffer arg0) {
                buffer.appendBuffer(arg0);
							}
						});

						resp.endHandler(new Handler<Void>() {
              @Override
              public void handle(Void arg0) {
                doneSignal.countDown();        
              }
						});

					}
				});
		
		req.putHeader(CONTENT_LENGTH, outRequest.length()+"");
		req.end(outRequest);
		doneSignal.await();
    return mapper.readValue(buffer.getBytes(), IntraRes.class);
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
