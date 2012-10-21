package org.usergrid.vx.handler.net;

import java.util.UUID;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.vx.util.UserFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

public class SmileClientNetHandler  implements Handler<NetSocket> {
	private final Logger logger = LoggerFactory.getLogger(SmileClientNetHandler.class);
	
	final int packetSize = 32 * 1024;
    
	static SmileFactory smile = new SmileFactory();

    static ObjectMapper smileMapper = new ObjectMapper(smile);
    

	
	@Override
	public void handle(NetSocket socket) {			
		
		sendData(socket, UserFactory.buildUser(UUID.randomUUID()));
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



}
