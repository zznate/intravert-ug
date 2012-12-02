package org.usergrid.vx.client;

import java.nio.ByteBuffer;

import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.thrift.protocol.TJSONProtocol;
import org.apache.thrift.protocol.TProtocol;
import org.apache.thrift.transport.THttpClient;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpClientResponse;

public class ThriftClient implements Handler<HttpClientResponse> {

	public static void main(String[] args) throws Exception {
		String servletUrl = "http://localhost:8080/:appid/thriftjson";

		THttpClient thc = new THttpClient(servletUrl);
		TProtocol loPFactory = new TJSONProtocol(thc);
		Cassandra.Client client = new Cassandra.Client(loPFactory);

		Column col = new Column();
		col.setName("mycol".getBytes());
		col.setValue("bla".getBytes());
		col.setTimestamp(System.nanoTime());
		
		client.insert(ByteBuffer.wrap("abc".getBytes()), new ColumnParent().setColumn_family("family"), col, ConsistencyLevel.ONE);

	}
	@Override
	public void handle(HttpClientResponse arg0) {
		// TODO Auto-generated method stub
		
	}

	

}
