package org.usergrid.vx.handler.http;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.usergrid.vx.util.UserFactory;
import org.usergrid.vx.util.UserHolder;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;

public class UserHttpClient implements Handler<HttpClientResponse> {
	
	private Logger logger = LoggerFactory.getLogger(UserHttpClient.class);
	
	private final HttpClient httpClient;
	
	private final Vertx vertx;
	
	private final AtomicLong counter = new AtomicLong(0);
	
	private int limit = 5000;
	
	public UserHttpClient(Vertx vertx) {
		this.vertx = vertx;
		this.httpClient = vertx.createHttpClient().setHost("localhost").setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
	}
	
	public void start() {
		makeRequest(1);
		logger.info("userClient started");
	}
	
	public boolean limitHit() {
		return counter.get() >= limit;
	}
	
	@Override
	public void handle(HttpClientResponse response) {


		response.endHandler(new SimpleHandler() {

			public void handle() {
				if ( counter.get() < limit ) {
					logger.info("in endHandler");
					makeRequest(10);
				}
			}
		});

	}
	
	public void makeRequest(int batch) {
		
		long count = counter.incrementAndGet();
		UserHolder uh = new UserHolder();
		for (int x=0; x<batch; x++){
			uh.addUser(UserFactory.buildUser(UUID.randomUUID()));
		}
		logger.info("makng request {}", count);
		HttpClientRequest req = httpClient.request("POST", "/:appid/users", this);		
		String value = JsonUtils.mapToJsonString(uh);
		req.putHeader("content-length", value.length());
		req.write(value);
		req.end();
		
	}

}
