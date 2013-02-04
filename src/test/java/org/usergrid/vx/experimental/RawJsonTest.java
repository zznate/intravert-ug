package org.usergrid.vx.experimental;

import java.util.concurrent.CountDownLatch;

import org.apache.cassandra.config.Schema;
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

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class RawJsonTest {

    private static Logger logger = LoggerFactory.getLogger(IntraClient.class);
    private static Vertx vertx;
    private HttpClient httpClient;

    @Before
    public void setup() {
        vertx = Vertx.newVertx();
        this.httpClient = vertx.createHttpClient().setHost("localhost")
            .setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
    }

    @Test
    public void createKeyspaceViaCQL() throws Exception {
        String post =
            "{\"e\":[" +
                "{ " +
                " \"type\": \"SETKEYSPACE\", " +
                " \"op\": { " +
                " \"keyspace\": \"system\" " +
                " } " +
                "}, " +
                " { " +
                "   \"type\":\"CQLQUERY\", " +
                "  \"op\": { " +
                "     \"version\": \"3.0.0\", " +
                "     \"query\": \"CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\" " +
                "  } " +
                " } " +
                " ]} ";
        System.out.println("posting " + post);
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
        req.putHeader("content-length", post.length());
        req.write(post);
        req.end();
        doneSignal.await();

        Assert.assertNotNull(Schema.instance.getKSMetaData("simple"));
        Assert.assertNotNull("Failed to find keyspace", Schema.instance.getTableDefinition("simple"));
    }

    @Test
    public void setAndGetColumn() throws Exception {
        String setColumnJSON =
            "{\"e\": [" +
                "  {" +
                "    \"type\": \"SETKEYSPACE\"," +
                "    \"op\": {" +
                "      \"keyspace\": \"myks\"" +
                "    }" +
                "  }," +
                "  {" +
                "    \"type\": \"SETCOLUMNFAMILY\"," +
                "    \"op\": {" +
                "      \"columnfamily\": \"mycf\"" +
                "    }" +
                "  }," +
                "  {" +
                "    \"type\": \"SET\"," +
                "    \"op\": {" +
                "      \"rowkey\": \"row_key1\"," +
                "      \"name\": \"column1\"," +
                "      \"value\": \"value1\"" +
                "    }" +
                "  }" +
                "]}";
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

        final String getColumnJSON =
            "{\"e\": [" +
                "  {" +
                "    \"type\": \"SETKEYSPACE\"," +
                "    \"op\": {" +
                "      \"keyspace\": \"myks\"" +
                "    }" +
                "  }," +
                "  {" +
                "    \"type\": \"SETCOLUMNFAMILY\"," +
                "    \"op\": {\n" +
                "      \"columnfamily\": \"mycf\"" +
                "    }" +
                "  }," +
                "{" +
                "    \"type\": \"ASSUME\"," +
                "    \"op\": {\n" +
                "      \"keyspace\": \"myks\"," +
                "      \"columnfamily\": \"mycf\"," +
                "      \"type\": \"column\"," +
                "      \"clazz\": \"UTF-8\"" +
                "    }" +
                "  }," +
                "{" +
                "    \"type\": \"ASSUME\"," +
                "    \"op\": {" +
                "      \"keyspace\": \"myks\"," +
                "      \"columnfamily\": \"mycf\"," +
                "      \"type\": \"value\"," +
                "      \"clazz\": \"UTF-8\"" +
                "    }" +
                "  }," +
                "  {" +
                "    \"type\": \"GET\"," +
                "    \"op\": {" +
                "      \"rowkey\": \"row_key1\"," +
                "      \"name\": \"column1\"" +
                "    }" +
                "  }" +
                "]}";
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

        String expectedResponse =
            "{\"exception\":null,\"exceptionId\":null,\"opsRes\":{\"0\":\"OK\",\"1\":\"OK\",\"2\":\"OK\",\"3\":\"OK\",\"4\":[{\"name\":\"column1\",\"value\":\"value1\"}]}}";

        Assert.assertEquals("The response was incorrect", expectedResponse, data.toString());
    }
}
