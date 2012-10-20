package org.usergrid.vx;

import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.entities.User;
import org.usergrid.utils.JsonUtils;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetClient;
import org.vertx.java.core.net.NetSocket;

/**
 * 
 *
 */
public class App {
	private static Logger logger = LoggerFactory.getLogger(App.class);
	
	private static Vertx vertx;
	private static NetClient client;
	
	static SmileFactory smile = new SmileFactory();

    static ObjectMapper smileMapper = new ObjectMapper(smile);
	
    public static void main( String[] args ) throws Exception {
    	vertx = Vertx.newVertx();
    	client = vertx.createNetClient();
        
    	App app = new App();
    	app.doCreateUser();
    	Thread.currentThread().sleep(5000);
    }
    
    private void doCreateUser() {
    	final User user = new User();
    	user.setUsername("zznate");
    	user.setEmail("nate@apigee.com");
    	user.setProperty("password", "password");
    	client.connect(8001, new Handler<NetSocket>() {
			
			@Override
			public void handle(NetSocket event) {
				try {
					logger.error("writing user...");
				event.write(new Buffer(smileMapper.writeValueAsBytes(user)), new Handler<Void>() {
					public void handle(Void v) {
						logger.error("done writing user");
					}
				});
				} catch (Exception ex) {
					ex.printStackTrace();
				}
				
			}
		});
 
    	client.close();
    }
}
