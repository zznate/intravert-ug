package org.usergrid.vx.handler;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

public class UserMutationHandler implements Handler<NetSocket> {
	private static Logger logger = LoggerFactory.getLogger(UserMutationHandler.class);
	
	static SmileFactory smile = new SmileFactory();

    static ObjectMapper smileMapper = new ObjectMapper(smile);
	
	@Override
	public void handle(NetSocket sock) {
		logger.error("handling data...");
		sock.dataHandler(new Handler<Buffer>() {
			
			@Override
			public void handle(Buffer event) {
				logger.error("have buffer: {}", event);	
				User user;
				try{ 
					user = smileMapper.readValue(event.getBytes(), User.class);
					logger.error("found User: " + user);
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				logger.error("event handle complete");
			}
			
		});

		
	}


}
