package org.usergrid.vx;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.usergrid.vx.handler.SmileClientHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;
import org.vertx.java.core.streams.Pump;

/**
 * 
 *
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);
	
	private static Vertx vertx;
	private static NetClient client;
	private static SmileClientHandler smileHandler;
	

	
    public static void main( String[] args ) throws Exception {
    	vertx = Vertx.newVertx();
    	client = vertx.createNetClient();
        smileHandler = new SmileClientHandler();
       
        client.connect(8001, smileHandler);
        
    	
        Thread.currentThread().sleep(5000);
    	
    	
    	client.close();
    	
    	
    	
    	
    	Thread.currentThread().sleep(5000);
    }
    

    

    
  

}
