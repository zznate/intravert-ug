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
import org.usergrid.vx.handler.RequestJsonHandler;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.usergrid.vx.handler.rest.ColumnFamilyRestHandler;
import org.usergrid.vx.handler.rest.ColumnRestHandler;
import org.usergrid.vx.handler.rest.KeyspaceMetaHandler;
import org.usergrid.vx.handler.rest.SystemMetaHandler;
import org.usergrid.vx.server.operations.AssumeHandler;
import org.usergrid.vx.server.operations.AutotimestampHandler;
import org.usergrid.vx.server.operations.BatchHandler;
import org.usergrid.vx.server.operations.ComponentSelectHandler;
import org.usergrid.vx.server.operations.ConsistencyHandler;
import org.usergrid.vx.server.operations.CounterHandler;
import org.usergrid.vx.server.operations.CqlQueryHandler;
import org.usergrid.vx.server.operations.CreateColumnFamilyHandler;
import org.usergrid.vx.server.operations.CreateFilterHandler;
import org.usergrid.vx.server.operations.CreateKeyspaceHandler;
import org.usergrid.vx.server.operations.CreateMultiProcessHandler;
import org.usergrid.vx.server.operations.CreateProcessorHandler;
import org.usergrid.vx.server.operations.FilterModeHandler;
import org.usergrid.vx.server.operations.GetHandler;
import org.usergrid.vx.server.operations.ListColumnFamilyHandler;
import org.usergrid.vx.server.operations.ListKeyspacesHandler;
import org.usergrid.vx.server.operations.SetColumnFamilyHandler;
import org.usergrid.vx.server.operations.SetHandler;
import org.usergrid.vx.server.operations.SetKeyspaceHandler;
import org.usergrid.vx.server.operations.SliceHandler;
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
    rm.post(String.format("%s/intrareq-jsonsmile", basePath), new IntraHandlerJsonSmile(vertx));

    SystemMetaHandler systemMetaHandler = new SystemMetaHandler(vertx);
    KeyspaceMetaHandler keyspaceMetaHandler = new KeyspaceMetaHandler(vertx);
    ColumnFamilyRestHandler columnFamilyRestHandler = new ColumnFamilyRestHandler(vertx);
    ColumnRestHandler columnRestHandler = new ColumnRestHandler(vertx);

    rm.get(String.format("%s/intrareq-rest/", basePath),systemMetaHandler);
    rm.get(String.format("%s/intrareq-rest/:ks/", basePath), keyspaceMetaHandler);
    rm.post(String.format("%s/intrareq-rest/:ks/",basePath), keyspaceMetaHandler);
    rm.delete(String.format("%s/intrareq-rest/:ks/", basePath), keyspaceMetaHandler);
    rm.post(String.format("%s/intrareq-rest/:ks/:cf", basePath), columnFamilyRestHandler);
    rm.post(String.format("%s/intrareq-rest/:ks/:cf/:row/:col", basePath), columnRestHandler);
    rm.get(String.format("%s/intrareq-rest/:ks/:cf/:row/:col", basePath), columnRestHandler);

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
    x.eventBus().registerHandler(RequestJsonHandler.IHJSON_HANDLER_TOPIC,
            new RequestJsonHandler(vertx));
  }
   
  public static void registerOperationHandlers(Vertx x) {
    x.eventBus().registerHandler("request.autotimestamp", new AutotimestampHandler() );
    x.eventBus().registerHandler("request.batchset", new BatchHandler() );
    x.eventBus().registerHandler("request.componentselect", new ComponentSelectHandler() );
    x.eventBus().registerHandler("request.createkeyspace", new CreateKeyspaceHandler());
    x.eventBus().registerHandler("request.setkeyspace", new SetKeyspaceHandler());
    x.eventBus().registerHandler("request.createcolumnfamily", new CreateColumnFamilyHandler());
    x.eventBus().registerHandler("request.listkeyspaces", new ListKeyspacesHandler());
    x.eventBus().registerHandler("request.listcolumnfamily", new ListColumnFamilyHandler());
    x.eventBus().registerHandler("request.set", new SetHandler());
    x.eventBus().registerHandler("request.setcolumnfamily", new SetColumnFamilyHandler());
    x.eventBus().registerHandler("request.assume", new AssumeHandler());
    x.eventBus().registerHandler("request.get", new GetHandler(x.eventBus()));
    x.eventBus().registerHandler("request.slice", new SliceHandler(x.eventBus()));
    x.eventBus().registerHandler("request.cqlquery", new CqlQueryHandler());
    x.eventBus().registerHandler("request.counter", new CounterHandler());
    x.eventBus().registerHandler("request.consistency", new ConsistencyHandler());
    x.eventBus().registerHandler("request.createfilter", new CreateFilterHandler(x.eventBus()));
    x.eventBus().registerHandler("request.createprocessor", new CreateProcessorHandler(x.eventBus()));
    x.eventBus().registerHandler("request.filtermode", new FilterModeHandler());
    x.eventBus().registerHandler("request.createmultiprocess", new CreateMultiProcessHandler(x.eventBus()));
  }

}
