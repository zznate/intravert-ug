package org.usergrid.vx.experimental;

import java.beans.XMLEncoder;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

public class IntraHandlerXml implements Handler<HttpServerRequest>{

	static IntraService is = new IntraService();
	@Override
	public void handle(final HttpServerRequest request) {
		final IntraRes res = new IntraRes();
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
				ByteArrayInputStream i = new ByteArrayInputStream(buffer.getBytes());
				java.beans.XMLDecoder d = new java.beans.XMLDecoder(i);
				IntraReq req = (IntraReq) d.readObject();
				
				is.handleIntraReq(req,res);
				
				ByteArrayOutputStream bo = new ByteArrayOutputStream();
				XMLEncoder e = new XMLEncoder(bo);
		    	e.writeObject(res);
		    	e.close();
		    	String payload = new String(bo.toByteArray());
		    	request.response.end(payload);
			}
		});
	}
	
	
	
}
