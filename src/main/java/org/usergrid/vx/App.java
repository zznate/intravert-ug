package org.usergrid.vx;

import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.usergrid.vx.handler.http.UserHttpClient;
import org.usergrid.vx.handler.http.UserInsertHttpHandler;
import org.usergrid.vx.handler.net.SmileClientNetHandler;
import org.usergrid.vx.util.UserFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
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
	//private static NetClient client;
	private static SmileClientNetHandler smileHandler;
	

	
    public static void main( String[] args ) throws Exception {
    	vertx = Vertx.newVertx();
    	//client = vertx.createNetClient();
        //smileHandler = new SmileClientNetHandler();
       
        //client.connect(8001, smileHandler);
        
        
    	AtomicLong counter = new AtomicLong();
        
        long startTime = System.currentTimeMillis();
        UserHttpClient userClient = null;
        
        //for (int x=0; x<1000; x++){
        try{ 
        	
        	userClient = new UserHttpClient(vertx);
        	
        	userClient.start();

       
        	counter.incrementAndGet();
        	
        
        } catch (Exception ex) {
        	ex.printStackTrace();
        } finally {
        	
        }
        // }
    
    	while ( !userClient.limitHit() ) { 
    		Thread.currentThread().sleep(20);
    	}
    	long endTime = System.currentTimeMillis() - startTime;
    	
    	logger.info("completed {} reqs ({}ms)", counter.get(), endTime);
    	
    	
    }
    

    

    
  

}
