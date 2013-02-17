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

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraHandlerJson;
import org.usergrid.vx.experimental.IntraHandlerJsonSmile;
import org.usergrid.vx.experimental.IntraHandlerXml;
import org.usergrid.vx.handler.http.HelloHandler;
import org.usergrid.vx.handler.http.NoMatchHandler;
import org.usergrid.vx.handler.http.ThriftHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.RouteMatcher;
import org.vertx.java.core.json.JsonObject;

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
		rm.noMatch(new NoMatchHandler());

            registerOperationHandlers();

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

    private void registerOperationHandlers() {
        vertx.eventBus().registerHandler("request.createkeyspace", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                JsonObject params = event.body.getObject("op");
                Integer id = event.body.getInteger("id");
                JsonObject state = event.body.getObject("state");
                String keyspace = params.getString("name");
                int replication = params.getInteger("replication");

                JsonObject response = new JsonObject();

                Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(0);
                KsDef def = new KsDef();
                def.setName(keyspace);
                def.setStrategy_class("SimpleStrategy");
                Map<String, String> strat = new HashMap<String, String>();
                //TODO we should be able to get all this information from the client
                strat.put("replication_factor", Integer.toString(replication));
                def.setStrategy_options(strat);
                KSMetaData ksm = null;
                try {
                    ksm = KSMetaData.fromThrift(def,
                        cfDefs.toArray(new CFMetaData[cfDefs.size()]));
                } catch (ConfigurationException e) {
                    response.putString("exception", e.getMessage());
                    response.putNumber("exceptionId", id);
                    event.reply(response);
                    return;
                }

                try {
                    MigrationManager.announceNewKeyspace(ksm);
                } catch (ConfigurationException e) {
                    response.putString("exception", e.getMessage());
                    response.putNumber("exceptionId", id);
                    event.reply(response);
                    return;
                }

                response.putString(id.toString(), "OK");
                event.reply(response);
            }
        });

        vertx.eventBus().registerHandler("request.setkeyspace", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                Integer id = event.body.getInteger("id");
                JsonObject params = event.body.getObject("op");
                JsonObject state = event.body.getObject("state");
                state.putString("currentKeyspace", params.getString("keyspace"));

                event.reply(new JsonObject()
                    .putString(id.toString(), "OK")
                    .putObject("state", state)
                );
            }
        });

        vertx.eventBus().registerHandler("request.createcolumnfamily", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                Integer id = event.body.getInteger("id");
                JsonObject params = event.body.getObject("op");
                JsonObject state = event.body.getObject("state");

                JsonObject response = new JsonObject();

                String cf = params.getString("name");
                CfDef def = new CfDef();
                def.setName(cf);
                def.setKeyspace(state.getString("currentKeyspace"));
                def.unsetId();
                CFMetaData cfm = null;

                try {
                    cfm = CFMetaData.fromThrift(def);
                    cfm.addDefaultIndexNames();
                } catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
                    response.putString("exception", e.getMessage());
                    response.putString("exceptionId", id.toString());
                    event.reply(response);
                    return;
                } catch (ConfigurationException e) {
                    response.putString("exception", e.getMessage());
                    response.putString("exceptionId", id.toString());
                    event.reply(response);
                    return;
                }
                try {
                    MigrationManager.announceNewColumnFamily(cfm);
                } catch (ConfigurationException e) {
                    response.putString("exception", e.getMessage());
                    response.putString("exceptionId", id.toString());
                    event.reply(response);
                    return;
                }
                response.putString(id.toString(), "OK");
                event.reply(response);
            }
        });
    }

}
