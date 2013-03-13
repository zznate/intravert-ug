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
		String servletUrl = "http://localhost:8080/intravert/thriftjson";

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
