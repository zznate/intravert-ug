package org.usergrid.vx.experimental;

import static org.junit.Assert.*;

import java.beans.XMLDecoder;
import java.io.ByteArrayInputStream;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Handler;
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
	public void setup () {
		vertx = Vertx.newVertx();
		this.httpClient = vertx.createHttpClient().setHost("localhost")
				.setPort(8080).setMaxPoolSize(1).setKeepAlive(true);
	}
	
	@Test
	public void simpleRequest() throws Exception {
	  String post = 	
		"{\"e\":["+
			 "{ "+
		     " \"type\": \"SETKEYSPACE\", "+
		     " \"op\": { "+
		     " \"keyspace\": \"system\" "+
		     " } "+
		     "}, "+
		     " { "+
		     "   \"type\":\"CQLQUERY\", "+
		     "  \"op\": { "+
		     "     \"version\": \"3.0.0\", "+
		     "     \"query\": \"CREATE KEYSPACE simple WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}\" "+
		     "  } "+
		     " } "+
		    " ]} ";
	  System.out.println("posting "+ post);
	  HttpClientRequest req = httpClient.request("POST", "/:appid/intrareq-json", new Handler<HttpClientResponse>(){
		@Override
		public void handle(HttpClientResponse resp) {
			resp.dataHandler(new Handler<Buffer>(){
				@Override
				public void handle(Buffer arg0) {
					System.out.println(	new String(arg0.getBytes()) );
				}
			});
		}
	  });
	  req.putHeader("content-length", post.length());
	  req.write(post);
	  req.end();
	  Thread.sleep(4000);
	}
}
