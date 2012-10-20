package org.usergrid.vx.handler;

import static java.util.Arrays.asList;
import static me.prettyprint.hector.api.factory.HFactory.createColumn;
import static org.usergrid.persistence.Schema.PROPERTY_CREATED;
import static org.usergrid.persistence.Schema.PROPERTY_MODIFIED;
import static org.usergrid.persistence.Schema.PROPERTY_TIMESTAMP;
import static org.usergrid.persistence.Schema.PROPERTY_TYPE;
import static org.usergrid.persistence.Schema.PROPERTY_UUID;
import static org.usergrid.persistence.Schema.TYPE_APPLICATION;
import static org.usergrid.persistence.Schema.getDefaultSchema;
import static org.usergrid.persistence.Schema.serializeEntityProperty;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.addInsertToMutator;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.key;
import static org.usergrid.utils.ConversionUtils.bytebuffer;
import static org.usergrid.utils.UUIDUtils.getTimestampInMicros;
import static org.usergrid.utils.UUIDUtils.newTimeUUID;

import java.nio.ByteBuffer;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicLong;

import me.prettyprint.hector.api.beans.HColumn;

import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.smile.SmileFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.persistence.Schema;
import org.usergrid.persistence.IndexBucketLocator.IndexType;
import org.usergrid.persistence.cassandra.ApplicationCF;
import org.usergrid.persistence.cassandra.SimpleIndexBucketLocatorImpl;
import org.usergrid.persistence.entities.User;
import org.usergrid.persistence.schema.CollectionInfo;
import org.usergrid.utils.JsonUtils;
import org.usergrid.vx.service.UserMutator;
import org.vertx.java.core.Handler;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.net.NetSocket;

public class UserMutationHandler implements Handler<NetSocket> {
	private static Logger logger = LoggerFactory.getLogger(UserMutationHandler.class);
	
	static SmileFactory smile = new SmileFactory();

    static ObjectMapper smileMapper = new ObjectMapper(smile);
    
    private UUID applicationId = UUID.randomUUID();
    
    // TODO add injected annotation
    private UserMutator userMutator;
    private AtomicLong counter = new AtomicLong();
    
    public UserMutationHandler() {
    	this.userMutator = new UserMutator();
    }

    @Override
    public void handle(NetSocket sock) {
    	logger.error("handling data...");


    	sock.dataHandler(new Handler<Buffer>() {

    		@Override
    		public void handle(Buffer event) {
    			// logger.error("have buffer: {}", event);	
    			extractUser(event);
    		}
    	});

    }

    private void extractUser(Buffer userData) {
    	User user = null;
    	try{ 

    		user = smileMapper.readValue(userData.getBytes(), User.class);


    	} catch (Exception ex) {
    		ex.printStackTrace();

    	}
    	if ( user != null ) {
    		userMutator.insert(applicationId, user);
    		counter.incrementAndGet();
    		logger.error("found User: " + user);

    		logger.error("event handle complete, current count {}", counter.get());
    	}
    }			

}
