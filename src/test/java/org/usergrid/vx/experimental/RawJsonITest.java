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

import static java.util.Arrays.asList;

import java.io.BufferedInputStream;
import java.io.ByteArrayOutputStream;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.config.Schema;
import org.apache.cassandra.service.MigrationManager;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
import org.vertx.java.core.SimpleHandler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpClient;
import org.vertx.java.core.http.HttpClientRequest;
import org.vertx.java.core.http.HttpClientResponse;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class RawJsonITest {

    private static Logger logger = LoggerFactory.getLogger(IntraClient.class);
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
        System.out.println("posting " + json);
        final CountDownLatch doneSignal = new CountDownLatch(1);
        HttpClientRequest req = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.dataHandler(new Handler<Buffer>() {
                    @Override
                    public void handle(Buffer arg0) {
                        System.out.println(new String(arg0.getBytes()));
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
        req.putHeader("content-length", json.length());
        req.write(json);
        req.end();
        doneSignal.await();

        Assert.assertNotNull(Schema.instance.getKSMetaData("simple"));
        Assert.assertNotNull("Failed to find keyspace", Schema.instance.getTableDefinition("simple"));
    }

    //@Test
    public void setAndGetColumn() throws Exception {
        String setColumnJSON = loadJSON("set_column.json");

        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
            @Override
            public void handle(HttpClientResponse resp) {
                resp.endHandler(new SimpleHandler() {
                    @Override
                    protected void handle() {
                    }
                });
            }
        });

        setReq.putHeader("content-length", setColumnJSON.length());
        setReq.write(setColumnJSON);
        setReq.end();

        final String getColumnJSON = loadJSON("get_column.json");

        final Buffer data = new Buffer(0);
        final HttpClientRequest getReq = httpClient.request("POST", "/:appid/intrareq-json",
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
        getReq.putHeader("content-length", getColumnJSON.length());
        getReq.write(getColumnJSON);
        getReq.end();
        doneSignal.await();

        String expectedResponse = loadJSON("get_column_response.json");

        assertJSONEquals("The response was incorrect", expectedResponse, data.toString());
    }

    //@Test
    public void executeColumnSliceQuery() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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

        final String getBeersJSON = loadJSON("get_beers.json");
        final Buffer data = new Buffer(0);
        final HttpClientRequest getReq = httpClient.request("POST", "/:appid/intrareq-json",
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
        String expectedResponse = loadJSON("beers_slice_response.json");

        assertJSONEquals("The response for the slice query was incorrect", expectedResponse, actualResponse);
    }

    //@Test
    public void setColumnUsingGetRef() throws Exception {
        String insertColumnsJSON = loadJSON("insert_columns_for_getref.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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
        final HttpClientRequest getReq = httpClient.request("POST", "/:appid/intrareq-json",
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

    //@Test
    public void filterColumnSlice() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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
        final HttpClientRequest getReq = httpClient.request("POST", "/:appid/intrareq-json",
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

    //@Test
    public void javascriptFilterColumnSlice() throws Exception {
        String insertBeersJSON = loadJSON("insert_beers.json");
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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
        final HttpClientRequest getReq = httpClient.request("POST", "/:appid/intrareq-json",
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

        final Buffer data = new Buffer();
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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

        setReq.putHeader("content-length", createKeyspaceJson.length());
        setReq.write(createKeyspaceJson);
        setReq.end();
        doneSignal.await();

        String actualResponse = data.toString();

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opRes", new JsonObject((Map) ImmutableMap.of("0", "OK")))
            .toString();

        assertJSONEquals("Failed to create keyspace", actualResponse, expectedResponse);
    }

    @Test
    @SuppressWarnings("unchecked")
    public void createColumnFamily() throws Exception {
        String createCFJson = loadJSON("create_cf.json");

        final Buffer data = new Buffer();
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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

        setReq.putHeader("content-length", createCFJson.length());
        setReq.write(createCFJson);
        setReq.end();
        doneSignal.await();

        String actualResponse = data.toString();

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opRes", new JsonObject((Map) ImmutableMap.of(
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

        final Buffer data = new Buffer();
        final CountDownLatch doneSignal = new CountDownLatch(1);
        final HttpClientRequest setReq = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>() {
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

        setReq.putHeader("content-length", listKeyspacesJSON.length());
        setReq.write(listKeyspacesJSON);
        setReq.end();
        doneSignal.await();

        String actualResponse = data.toString();

        String expectedResponse = new JsonObject()
            .putString("exception", null)
            .putString("exceptionId", null)
            .putObject("opRes", new JsonObject()
                .putString("0", "OK")
                .putString("1", "OK")
                .putArray("2", new JsonArray((List) asList("ks2", "myks", "ks1"))))
            .toString();

        assertJSONEquals("Failed to get keyspaces", expectedResponse, actualResponse);
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

    private void assertJSONEquals(String msg, String expected, String actual) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        JsonNode expectedJson = mapper.readTree(expected);
        JsonNode actualJson = mapper.readTree(actual);

        Assert.assertEquals(msg, expectedJson, actualJson);
    }
}
