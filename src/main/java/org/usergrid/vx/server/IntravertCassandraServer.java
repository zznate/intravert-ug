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
import org.usergrid.vx.handler.PayloadRoutingHandler;
import org.usergrid.vx.handler.http.payload.IntraHandlerJson;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.usergrid.vx.handler.http.rest.KeyspaceMetaHandler;
import org.usergrid.vx.handler.http.rest.SystemMetaHandler;
import org.usergrid.vx.server.operations.*;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.http.RouteMatcher;

public class IntravertCassandraServer implements CassandraDaemon.Server {
  private static final int PORT = 8080;
  private final Logger logger = LoggerFactory.getLogger(IntravertCassandraServer.class);
  private static Vertx vertx;
  private static RouteMatcher rm;
  private static IntravertClusterNotifier intravertClusterNotifier;
  private static final AtomicBoolean running = new AtomicBoolean(false);
  private final String basePath;

  public IntravertCassandraServer(String basePath) {
    this.basePath = basePath;
  }


  @Override
  public void start() {
    logger.info("Starting IntravertCassandraServer with base path {}", basePath);
    vertx = Vertx.newVertx();
    rm = new RouteMatcher();
    // TODO Should we use a single instance of HelloHandler here?
    rm.put(String.format("%s/hello", basePath), new HelloHandler());
    rm.get(String.format("%s/hello", basePath), new HelloHandler());
    rm.post(String.format("%s/hello", basePath), new HelloHandler());
    rm.post(String.format("%s/intrareq-json", basePath), new IntraHandlerJson(vertx));
    //rm.post(String.format("%s/intrareq-jsonsmile", basePath), new IntraHandlerJsonSmile(vertx));

    SystemMetaHandler systemMetaHandler = new SystemMetaHandler(vertx);
    KeyspaceMetaHandler keyspaceMetaHandler = new KeyspaceMetaHandler(vertx);

    rm.get(String.format("%s/intrareq-rest/", basePath),systemMetaHandler);
    rm.get(String.format("%s/intrareq-rest/:ks/", basePath), keyspaceMetaHandler);
    rm.post(String.format("%s/intrareq-rest/:ks/",basePath), keyspaceMetaHandler);
    rm.delete(String.format("%s/intrareq-rest/:ks/", basePath),keyspaceMetaHandler);

    rm.noMatch(new NoMatchHandler());
    registerOperationHandlers(vertx);
    registerRequestHandler(vertx);
    vertx.createHttpServer().requestHandler(rm).listen(PORT);
    logger.info("IntravertCassandraServer started, listening on [" + PORT + "]");
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

  public static void registerRequestHandler(Vertx x) {
    x.eventBus().registerHandler(PayloadRoutingHandler.IHJSON_HANDLER_TOPIC,
            new PayloadRoutingHandler(vertx));
  }
   
  public static void registerOperationHandlers(Vertx x) {
    x.eventBus().registerHandler("operations.autotimestamp", new AutotimestampHandler() );
    x.eventBus().registerHandler("operations.batchset", new BatchHandler() );
    x.eventBus().registerHandler("operations.componentselect", new ComponentSelectHandler() );
    x.eventBus().registerHandler("operations.createkeyspace", new CreateKeyspaceHandler());
    x.eventBus().registerHandler("operations.setkeyspace", new SetKeyspaceHandler());
    x.eventBus().registerHandler("operations.createcolumnfamily", new CreateColumnFamilyHandler());
    x.eventBus().registerHandler("operations.listkeyspaces", new ListKeyspacesHandler());
    x.eventBus().registerHandler("operations.listcolumnfamily", new ListColumnFamilyHandler());
    x.eventBus().registerHandler("operations.set", new SetHandler());
    x.eventBus().registerHandler("operations.setcolumnfamily", new SetColumnFamilyHandler());
    x.eventBus().registerHandler("operations.assume", new AssumeHandler());
    x.eventBus().registerHandler("operations.get", new GetHandler(x.eventBus()));
    x.eventBus().registerHandler("operations.slice", new SliceHandler(x.eventBus()));
    x.eventBus().registerHandler("operations.cqlquery", new CqlQueryHandler());
    x.eventBus().registerHandler("operations.counter", new CounterHandler());
    x.eventBus().registerHandler("operations.consistency", new ConsistencyHandler());
    x.eventBus().registerHandler("operations.createfilter", new CreateFilterHandler(x.eventBus()));
    x.eventBus().registerHandler("operations.createprocessor", new CreateProcessorHandler(x.eventBus()));
    x.eventBus().registerHandler("operations.filtermode", new FilterModeHandler());
    x.eventBus().registerHandler("operations.createmultiprocess", new CreateMultiProcessHandler(x.eventBus()));
    x.eventBus().registerHandler("operations.createserviceprocess", new CreateServiceProcessHandler(x.eventBus()));
  }

}
