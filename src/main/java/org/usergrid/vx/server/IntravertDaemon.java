package org.usergrid.vx.server;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.RouteMatcher;

public class IntravertDaemon extends CassandraDaemon {
	
	private final Logger logger = LoggerFactory.getLogger(IntravertDaemon.class);
	private static Vertx vertx;
	private static RouteMatcher rm;
	
	@Override
	public void start() {
    // TODO may be more appropriate in setup() ?
		vertx = Vertx.newVertx();
		rm = new RouteMatcher();
		rm.put("/:appid/hello", new HelloHandler());
		rm.get("/:appid/hello", new HelloHandler());
		rm.post("/:appid/hello", new HelloHandler());
		rm.noMatch(new NoMatchHandler() );
		vertx.createHttpServer().requestHandler(rm).listen(8080);
		logger.info("startServer...");
	}

	@Override
	public void stop() {
		logger.info("stopServer...");		
	}

}
