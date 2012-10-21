package org.usergrid.vx.handler.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.http.HttpServerRequest;

public class UserHttpClient implements Handler<HttpClientResponse> {
	
	private Logger logger = LoggerFactory.getLogger(UserHttpClient.class);
	
	@Override
	public void handle(HttpClientResponse response) {
		logger.info("handling response: {}", response);

	}
	
	public void writeUser(HttpClientRequest request, User user) {
		String value = JsonUtils.mapToJsonString(user);
		request.putHeader("content-length", value.length());
		request.write(value);
		request.end();
	}

}
