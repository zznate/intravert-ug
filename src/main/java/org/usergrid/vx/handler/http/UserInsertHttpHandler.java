package org.usergrid.vx.handler.http;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.usergrid.vx.service.UserMutator;
import org.usergrid.vx.util.UserHolder;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

public class UserInsertHttpHandler implements Handler<HttpServerRequest> {
	private Logger logger = LoggerFactory.getLogger(UserInsertHttpHandler.class);

	static ObjectMapper mapper = new ObjectMapper();
	
	private final UserMutator userMutator;
	
	public UserInsertHttpHandler() {
		userMutator = new UserMutator();
	}
	
	@Override
	public void handle(HttpServerRequest request) {
		Map<String, String> params = request.params();
		
		BodyHandler bodyHandler = new BodyHandler();
		
		request.bodyHandler(bodyHandler);
		
		request.response.end("ok");
			
	}
	
	class BodyHandler implements Handler<Buffer> {
		
		
		public void handle(Buffer event) {
			//logger.debug("received buffer: ");//, event.toString());
			
			try {
				UserHolder userHolder  = mapper.readValue(event.toString(), UserHolder.class);
				for ( User user : userHolder ) {
					userMutator.insert(new UUID(0, 1), user);
				}
			} catch (Exception e) {
				e.printStackTrace();
			} 
		}
	}

}
