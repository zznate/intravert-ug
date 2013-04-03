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
package org.usergrid.vx.experimental;

import com.google.common.collect.ImmutableMap;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.MigrationManager;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.client.IntraClient2;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import static java.util.Arrays.asList;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class RawJsonITest {

    private static Logger logger = LoggerFactory.getLogger(IntraClient2.class);
    private static Vertx vertx;
    private HttpClient httpClient;

    @Before
    public void setup() {
        vertx = Vertx.newVertx();
        this.httpClient = vertx.createHttpClient().setHost("localhost")
            .setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
    }

    //@Test
    public void createKeyspaceViaCQL() throws Exception {
        String json = loadJSON("create_keyspace_cql.json");
        submitRequest(json);

        Assert.assertNotNull(Schema.instance.getKSMetaData("simple"));
        Assert.assertNotNull("Failed to find keyspace", Schema.instance.getTableDefinition("simple"));
    }

    @Test
    public void executeCQL() throws Exception {
        String json = loadJSON("cql.json");
        String actualResponse = submitRequest(json);
        String expectedResponse = loadJSON("cql_response.json");

        assertJSONEquals("Failed to execute CQL commands", expectedResponse, actualResponse);
    }

    @Test
    public void setAndGetColumn() throws Exception {
        String setColumnJSON = loadJSON("set_column.json");
        submitRequest(setColumnJSON);

        String getColumnJSON = loadJSON("get_column.json");
        String actualResponse = submitRequest(getColumnJSON);
        String expectedResponse = loadJSON("get_column_response.json");

        assertJSONEquals("The response was incorrect", expectedResponse, actualResponse);
    }

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycountercf", isCounter = true)
  public void setAndGetCounter() throws Exception {
    String setCounterJSON = loadJSON("counter_set.json");
    submitRequest(setCounterJSON);

    String getCounterJSON = loadJSON("counter_get.json");
    String actualResponse = submitRequest(getCounterJSON);
    String expectedResponse = loadJSON("counter_get_response.json");

    assertJSONEquals("The counter response was incorrect", expectedResponse, actualResponse);
  }

    @Test
    public void executeColumnSliceQuery() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        submitRequest(insertBeersJSON);

        String getBeersJSON = loadJSON("get_beers.json");
        String actualResponse = submitRequest(getBeersJSON);
        String expectedResponse = loadJSON("beers_slice_response.json");

        assertJSONEquals("The response for the slice query was incorrect", expectedResponse, actualResponse);
    }

    //@Test
    public void setColumnUsingGetRef() throws Exception {
        String insertColumnsJSON = loadJSON("insert_columns_for_getref.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/intravert/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.endHandler(new SimpleHandler() {
                    @Override
                    protected void handle() {
                    }
                });
            }
        });

        setReq.putHeader("content-length", insertColumnsJSON.length());
        setReq.write(insertColumnsJSON);
        setReq.end();

        String getrefJSON = loadJSON("getref.json");
        final Buffer data = new Buffer(0);
        final HttpClientRequest getReq = httpClient.request("POST", "/intravert/intrareq-json",
            new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse resp) {
                    resp.dataHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer buffer) {
                            data.appendBuffer(buffer);
                        }
                    });

                    resp.endHandler(new SimpleHandler() {
                        @Override
                        protected void handle() {
                            doneSignal.countDown();
                        }
                    });
                }
            });
        getReq.putHeader("content-length", getrefJSON.length());
        getReq.write(getrefJSON);
        getReq.end();
        doneSignal.await();

        String actualResponse = data.toString();
        String expectedResponse = loadJSON("getref_response.json");

        assertJSONEquals("Failed to set column using GETREF", expectedResponse, actualResponse);
    }

    @Test
    public void filterColumnSlice() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/intravert/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.endHandler(new SimpleHandler() {
                    @Override
                    protected void handle() {
                    }
                });
            }
        });

        setReq.putHeader("content-length", insertBeersJSON.length());
        setReq.write(insertBeersJSON);
        setReq.end();

        final String getBeersJSON = loadJSON("filter_beers.json");
        final Buffer data = new Buffer(0);
        final HttpClientRequest getReq = httpClient.request("POST", "/intravert/intrareq-json",
            new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse resp) {
                    resp.dataHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer buffer) {
                            data.appendBuffer(buffer);
                        }
                    });

                    resp.endHandler(new SimpleHandler() {
                        @Override
                        protected void handle() {
                            doneSignal.countDown();
                        }
                    });
                }
            });
        getReq.putHeader("content-length", getBeersJSON.length());
        getReq.write(getBeersJSON);
        getReq.end();
        doneSignal.await();

        String actualResponse = data.toString();
        String expectedResponse = loadJSON("filter_beers_response.json");

        assertJSONEquals("Failed to apply filter", expectedResponse, actualResponse);
    }

    @Test
    public void javascriptFilterColumnSlice() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/intravert/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.endHandler(new SimpleHandler() {
                    @Override
                    protected void handle() {
                    }
                });
            }
        });

        setReq.putHeader("content-length", insertBeersJSON.length());
        setReq.write(insertBeersJSON);
        setReq.end();

        final String getBeersJSON = loadJSON("filter_beers_js.json");
        final Buffer data = new Buffer(0);
        final HttpClientRequest getReq = httpClient.request("POST", "/intravert/intrareq-json",
            new Handler<HttpClientResponse>() {
                @Override
                public void handle(HttpClientResponse resp) {
                    resp.dataHandler(new Handler<Buffer>() {
                        @Override
                        public void handle(Buffer buffer) {
                            data.appendBuffer(buffer);
                        }
                    });

                    resp.endHandler(new SimpleHandler() {
                        @Override
                        protected void handle() {
                            doneSignal.countDown();
                        }
                    });
                }
            });
        getReq.putHeader("content-length", getBeersJSON.length());
        getReq.write(getBeersJSON);
        getReq.end();
        doneSignal.await();

        String actualResponse = data.toString();
        String expectedResponse = loadJSON("filter_beers_response.json");

        assertJSONEquals("Failed to apply filter", expectedResponse, actualResponse);
    }

    @Test
    public void createKeyspace() throws Exception {
        String createKeyspaceJson = loadJSON("create_keyspace.json");

        String actualResponse = submitRequest(createKeyspaceJson);

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opsRes", new JsonObject((Map) ImmutableMap.of("0", "OK")))
            .toString();

        assertJSONEquals("Failed to create keyspace", expectedResponse, actualResponse);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createColumnFamily() throws Exception {
        String createCFJson = loadJSON("create_cf.json");

        String actualResponse = submitRequest(createCFJson);

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opsRes", new JsonObject((Map) ImmutableMap.of(
                "0", "OK",
                "1", "OK",
                "2", "OK"
            ))).toString();

        assertJSONEquals("Failed to create column family", expectedResponse, actualResponse);
    }

    @Test
    public void createAndListKeyspaces() throws Exception {
        for (String ks :Schema.instance.getTables()) {
            if (!ks.equals("myks") && !ks.equals("system")) {
                MigrationManager.announceKeyspaceDrop(ks);
            }
        }

        String listKeyspacesJSON = loadJSON("create_and_list_keyspaces.json");

        String actualResponse = submitRequest(listKeyspacesJSON);

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opsRes", new JsonObject()
                .putString("0", "OK")
                .putString("1", "OK")
                .putArray("2", new JsonArray((List) asList("myks", "ks1","ks2"))))
            .toString();

        assertJSONEquals("Failed to get keyspaces", expectedResponse, actualResponse);
    }

    @Test()
    @SuppressWarnings("unchecked")
    public void timeOutLongRunningOperation() throws Exception {
        // This test is specific to the async/timer version of timeouts so unless
        // the async-requests-enabled is true, we return to avoid a false failure.
        if (!Boolean.valueOf(System.getProperty("async-requests-enabled", "false"))) {
            return;
        }

        vertx.eventBus().registerHandler("request.noop", new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                try {
                    Thread.sleep(10050);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }

                Integer id = event.body.getInteger("id");
                event.reply(new JsonObject((Map) ImmutableMap.of(id.toString(), "OK")));
            }
        });

        JsonObject json = new JsonObject();
        JsonArray operations = new JsonArray();
        operations.addObject(new JsonObject()
            .putString("type", "NOOP")
            .putObject("op", new JsonObject()));
        json.putArray("e", operations);

        String timeoutJSON = json.toString();

        String actualResponse = submitRequest(timeoutJSON);

        String expectedResponse = new JsonObject()
            .putString("exception", "Operation timed out.")
            .putString("exceptionId", "0")
            .putObject("opsRes", new JsonObject())
            .toString();

        assertJSONEquals("Failed to time out long running operation", expectedResponse, actualResponse);
    }

  @Test
  public void overrideDefaultTimeoutForLongRunningOperation() throws Exception {
    vertx.eventBus().registerHandler("request.noop", new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        try {
          Thread.sleep(10050);
        } catch (InterruptedException e) {
          e.printStackTrace();
        }

        Integer id = event.body.getInteger("id");
        event.reply(new JsonObject((Map) ImmutableMap.of(id.toString(), "OK")));
      }
    });

    JsonObject json = new JsonObject();
    JsonArray operations = new JsonArray();
    operations.addObject(new JsonObject()
        .putString("type", "NOOP")
        .putObject("op", new JsonObject().putNumber("timeout", 12000)));
    json.putArray("e", operations);

    String timeoutJSON = json.toString();

    String actualResponse = submitRequest(timeoutJSON);

    String expectedResponse = new JsonObject()
        .putString("exception", "Operation timed out.")
        .putString("exceptionId", "0")
        .putObject("opsRes", new JsonObject())
        .toString();

    assertJSONEquals("Failed to time out long running operation", expectedResponse, actualResponse);
  }

    @Test
    public void handleBadRequest() throws Exception {
        String actualResponse = submitRequest(loadJSON("bad_request.json"));
        String expectedResponse = loadJSON("response_for_bad_request.json");

        assertJSONEquals("Failed to handle bad request", expectedResponse, actualResponse);
    }

    @Ignore
    @Test
    public void handleBadCQL() throws Exception {
        String actualResponse = submitRequest(loadJSON("bad_cql.json"));
        String expectedResponse = loadJSON("bad_cql_response.json");

        assertJSONEquals("Failed to report errors for bad CQL request", expectedResponse, actualResponse);
    }

    private String loadJSON(String file) throws Exception {
        try (
        	
            BufferedInputStream input = new BufferedInputStream(getClass().getResourceAsStream(file));
            ByteArrayOutputStream output = new ByteArrayOutputStream();
   
        ) {
            byte[] buffer = new byte[2048];
            
            for (int bytesRead = input.read(buffer); bytesRead != -1; bytesRead = input.read(buffer)) {
                output.write(buffer, 0, bytesRead);
            }
            output.flush();

            return new String(output.toByteArray());
        }
    }

    /**
     * Submits an HTTP request with <code>json</code> as the request body. This method will
     * block until the request has been fully processed and the response is received.
     *
     * @param json The request body
     * @return The results of the operations specified by the json argument
     * @throws InterruptedException
     */
    private String submitRequest(String json) throws InterruptedException {
        final Buffer data = new Buffer();
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/intravert/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer buffer) {
                        data.appendBuffer(buffer);
                    }
                });

                resp.endHandler(new SimpleHandler() {
                    @Override
                    protected void handle() {
                        doneSignal.countDown();
                    }
                });
            }
        });

        setReq.putHeader("content-length", json.length());
        setReq.write(json);
        setReq.end();
        doneSignal.await();

        return data.toString();
    }

    private void assertJSONEquals(String msg, String expected, String actual) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedJson = mapper.readTree(expected);
        JsonNode actualJson = mapper.readTree(actual);

        Assert.assertEquals(msg, expectedJson, actualJson);
    }
}
