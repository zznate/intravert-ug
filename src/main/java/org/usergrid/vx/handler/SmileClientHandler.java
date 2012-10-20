package org.usergrid.vx.handler;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

public class SmileClientHandler  implements Handler<NetSocket> {
	private final Logger logger = LoggerFactory.getLogger(SmileClientHandler.class);
	
	final int packetSize = 32 * 1024;
    
	static SmileFactory smile = new SmileFactory();

    static ObjectMapper smileMapper = new ObjectMapper(smile);
    

	
	@Override
	public void handle(NetSocket socket) {			
		
		sendData(socket, buildUser(1));
	}


	private void sendData(final NetSocket socket, final User user) {
		 Buffer data = new Buffer();
		 
		 try {			 
			 byte[] bytes = smileMapper.writeValueAsBytes(user);
			 data = new Buffer(bytes.length);
			 data.setBytes(0, bytes);
		 } catch (Exception ex) {
			 ex.printStackTrace();
		 }
		socket.write(data);
	}

	private static User buildUser(int x) {
		User user = new User();
		user.setUsername("zznate"+x);
		user.setEmail("nate" + x + "@apigee.com");
		user.setProperty("password", "password");
		return user;
	}

}
