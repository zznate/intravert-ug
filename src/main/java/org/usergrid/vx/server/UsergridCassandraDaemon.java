package org.usergrid.vx.server;

import org.apache.cassandra.service.AbstractCassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.handler.http.UserInsertHttpHandler;
import org.usergrid.vx.handler.net.UserMutationHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.net.NetServer;



public class UsergridCassandraDaemon extends AbstractCassandraDaemon {
	
	private final Logger logger = LoggerFactory.getLogger(UsergridCassandraDaemon.class);

	private static Vertx vertx;
	private static NetServer server;
	private static RouteMatcher rm;
	
	@Override
	protected void startServer() {
		vertx = Vertx.newVertx();
		
		rm = new RouteMatcher();
		rm.put("/:appid/users", new UserInsertHttpHandler());
		rm.post("/:appid/users", new UserInsertHttpHandler());
		
		vertx.createHttpServer().requestHandler(rm).listen(8080);
		
		server = vertx.createNetServer();
		
		server.connectHandler(new UserMutationHandler());
		server.listen(8001);
		
		logger.info("UCD startServer...");
		
	}

	@Override
	protected void stopServer() {
		logger.info("UCD stopServer...");
		
	}

	
}
