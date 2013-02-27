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

import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraHandlerJson;
import org.usergrid.vx.experimental.IntraHandlerJsonSmile;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.usergrid.vx.server.operations.AssumeHandler;
import org.usergrid.vx.server.operations.ConsistencyHandler;
import org.usergrid.vx.server.operations.CqlQueryHandler;
import org.usergrid.vx.server.operations.CreateColumnFamilyHandler;
import org.usergrid.vx.server.operations.CreateKeyspaceHandler;
import org.usergrid.vx.server.operations.GetHandler;
import org.usergrid.vx.server.operations.ListKeyspacesHandler;
import org.usergrid.vx.server.operations.SetColumnFamilyHandler;
import org.usergrid.vx.server.operations.SetHandler;
import org.usergrid.vx.server.operations.SetKeyspaceHandler;
import org.usergrid.vx.server.operations.SliceHandler;
import org.usergrid.vx.server.operations.*;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.RouteMatcher;

public class IntravertCassandraServer implements CassandraDaemon.Server {

  private final Logger logger = LoggerFactory.getLogger(IntravertCassandraServer.class);
  private static Vertx vertx;
  private static RouteMatcher rm;
  private static IntravertClusterNotifier intravertClusterNotifier;
  private static final AtomicBoolean running = new AtomicBoolean(false);

  @Override
  public void start() {
    logger.debug("Starting IntravertCassandraServer...");
    vertx = Vertx.newVertx();
    rm = new RouteMatcher();
    rm.put("/:appid/hello", new HelloHandler());
    rm.get("/:appid/hello", new HelloHandler());
    rm.post("/:appid/hello", new HelloHandler());
    rm.post("/:appid/intrareq-json", new IntraHandlerJson(vertx));
    rm.post("/:appid/intrareq-jsonsmile", new IntraHandlerJsonSmile(vertx));
    rm.noMatch(new NoMatchHandler());
    registerOperationHandlers(vertx);
    vertx.createHttpServer().requestHandler(rm).listen(8080);
    logger.info("IntravertCassandraServer started.");
    running.set(true);
    intravertClusterNotifier = IntravertClusterNotifier.forServer(vertx);
  }

  @Override
  public void stop() {
    boolean stopped = running.compareAndSet(true, false);
    logger.info("stopServer...{}", stopped);
  }

  @Override
  public boolean isRunning() {
    return running.get();
  }

  public static void registerOperationHandlers(Vertx x) {
    x.eventBus().registerHandler("request.createkeyspace", new CreateKeyspaceHandler());
    x.eventBus().registerHandler("request.setkeyspace", new SetKeyspaceHandler());
    x.eventBus().registerHandler("request.createcolumnfamily", new CreateColumnFamilyHandler());
    x.eventBus().registerHandler("request.listkeyspaces", new ListKeyspacesHandler());
    x.eventBus().registerHandler("request.set", new SetHandler());
    x.eventBus().registerHandler("request.setcolumnfamily", new SetColumnFamilyHandler());
    x.eventBus().registerHandler("request.assume", new AssumeHandler());
    x.eventBus().registerHandler("request.get", new GetHandler());
    x.eventBus().registerHandler("request.slice", new SliceHandler());
    x.eventBus().registerHandler("request.cqlquery", new CqlQueryHandler());
    x.eventBus().registerHandler("request.counter", new CounterHandler());
    x.eventBus().registerHandler("request.consistency", new ConsistencyHandler());
  }

}