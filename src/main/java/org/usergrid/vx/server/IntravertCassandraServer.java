/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.server;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraHandlerJsonSmile;
import org.usergrid.vx.experimental.IntraHandlerXml;
import org.usergrid.vx.experimental.IntraHandlerJson;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.usergrid.vx.handler.http.ThriftHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.RouteMatcher;

import java.util.concurrent.atomic.AtomicBoolean;

public class IntravertCassandraServer implements CassandraDaemon.Server {
	
	private final Logger logger = LoggerFactory.getLogger(IntravertCassandraServer.class);
	private static Vertx vertx;
	private static RouteMatcher rm;
  private static IntravertClusterNotifier intravertClusterNotifier;
  private static final AtomicBoolean running = new AtomicBoolean(false);
	
	@Override
	public void start() {
    logger.debug("Starting IntravertCassandraServer...");
    // TODO may be more appropriate in setup() ?
		vertx = Vertx.newVertx();
		
		rm = new RouteMatcher();
		rm.put("/:appid/hello", new HelloHandler());
		rm.get("/:appid/hello", new HelloHandler());
		rm.post("/:appid/hello", new HelloHandler());
		rm.post("/:appid/thriftjson", new ThriftHandler());
		rm.post("/:appid/intrareq-xml", new IntraHandlerXml(vertx));
		rm.post("/:appid/intrareq-json", new IntraHandlerJson(vertx));
		rm.post("/:appid/intrareq-jsonsmile", new IntraHandlerJsonSmile(vertx));
		rm.noMatch(new NoMatchHandler() );
		vertx.createHttpServer().requestHandler(rm).listen(8080);
		logger.info("IntravertCassandraServer started.");
    running.set(true);
    intravertClusterNotifier = IntravertClusterNotifier.forServer(vertx);
	}

	@Override
	public void stop() {
    boolean stopped = running.compareAndSet(true,false);
		logger.info("stopServer...{}", stopped);

	}

  @Override
  public boolean isRunning() {
    return running.get();
  }
}
