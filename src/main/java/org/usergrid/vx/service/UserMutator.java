package org.usergrid.vx.service;

import static org.usergrid.persistence.cassandra.ApplicationCF.ENTITY_COMPOSITE_DICTIONARIES;
import static org.usergrid.persistence.cassandra.ApplicationCF.ENTITY_DICTIONARIES;
import static org.usergrid.persistence.cassandra.ApplicationCF.ENTITY_ID_SETS;
import static org.usergrid.persistence.Schema.TYPE_APPLICATION;
import static org.usergrid.persistence.Schema.getDefaultSchema;
import static org.usergrid.persistence.cassandra.CassandraPersistenceUtils.key;
import static org.usergrid.utils.UUIDUtils.getTimestampInMicros;
import static org.usergrid.utils.UUIDUtils.newTimeUUID;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeoutException;

import me.prettyprint.hector.api.beans.DynamicComposite;

import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.usergrid.persistence.Schema;
import org.usergrid.persistence.IndexBucketLocator.IndexType;
import org.usergrid.persistence.cassandra.ApplicationCF;
import org.usergrid.persistence.cassandra.CassandraService;
import org.usergrid.persistence.cassandra.SimpleIndexBucketLocatorImpl;
import org.usergrid.persistence.entities.User;
import org.usergrid.persistence.schema.CollectionInfo;
import org.usergrid.utils.ConversionUtils;
import org.usergrid.utils.UUIDUtils;

public class UserMutator {
	
    private String eType = "user";
    private String collectionName = "users";
    private Set<String> required;
    private CollectionInfo collection;
    
    static SimpleIndexBucketLocatorImpl indexBucketLocator = new SimpleIndexBucketLocatorImpl(20);
    
    private Schema schema;
    
    public UserMutator() {
    	schema =  getDefaultSchema();
    	required = schema.getRequiredProperties(eType);
    	collection = schema.getCollection(TYPE_APPLICATION, collectionName);
    }
    
    private RowMutation rowMutationBuilder(ApplicationCF columnFamily, 
    		String key,
    		ByteBuffer columnName,
    		ByteBuffer columnValue,
    		long timestamp) {
    	RowMutation rm = new RowMutation(CassandraService.STATIC_APPLICATION_KEYSPACE, 
    			ByteBufferUtil.bytes(key));
    	rm.add(new QueryPath(columnFamily.getColumnFamily(), null, columnName), 
    			columnValue == null ? ByteBufferUtil.EMPTY_BYTE_BUFFER : columnValue, 
    					timestamp);
    	return rm;
    }
    
	public void insert(UUID applicationId, User user) {
		List<RowMutation> mutations = new ArrayList<RowMutation>();
		
		UUID timestampUuid = newTimeUUID();
		
		long timestamp = getTimestampInMicros(timestampUuid);
		
		Map<String,Object> properties = user.getProperties();										
		
		String bucketId = indexBucketLocator.getBucket(applicationId,
				IndexType.COLLECTION, timestampUuid, collectionName);
		
		Object collectionKey = key(applicationId, Schema.DICTIONARY_COLLECTIONS, collectionName, bucketId);
		
		// ENTITY_ID_SETS,
		
		// addInsertToMutator(m, ENTITY_ID_SETS, collectionKey, timestampUuid, null, timestamp);
		mutations.add(rowMutationBuilder(ENTITY_ID_SETS, collectionKey.toString(), 
				ConversionUtils.bytebuffer(timestampUuid), null, timestamp));
		//
		// // ENTITY_DICTIONARIES
		//addInsertToMutator(m, ENTITY_DICTIONARIES, key(applicationId, Schema.DICTIONARY_COLLECTIONS), collection_name, null, timestamp);
		mutations.add(rowMutationBuilder(ENTITY_DICTIONARIES, key(applicationId, Schema.DICTIONARY_COLLECTIONS).toString(), 
				ConversionUtils.bytebuffer(collectionName), null, timestamp));
		//
		// ENTITY_COMPOSITE_DICTIONARIES
		// addInsertToMutator(m, ENTITY_COMPOSITE_DICTIONARIES, key(itemId, Schema.DICTIONARY_CONTAINER_ENTITIES), 
		// asList(TYPE_APPLICATION, collection_name, applicationId),
		//		null, timestamp);
		mutations.add(rowMutationBuilder(ENTITY_COMPOSITE_DICTIONARIES, 
				key(timestampUuid, Schema.DICTIONARY_CONTAINER_ENTITIES).toString(), 
				DynamicComposite.toByteBuffer((List<?>) Arrays.asList(TYPE_APPLICATION, collectionName, applicationId)), 
				null, timestamp));
		
		// now add User properties by hand
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("username"),
				ConversionUtils.bytebuffer(user.getUsername()),
				timestamp));
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("email"),
				ConversionUtils.bytebuffer(user.getEmail()),
				timestamp));
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("created"),
				ConversionUtils.bytebuffer(timestamp/1000),
				timestamp));
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("modified"),
				ConversionUtils.bytebuffer(timestamp/1000),
				timestamp));
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("type"),
				ConversionUtils.bytebuffer("user"),
				timestamp));
		mutations.add(rowMutationBuilder(ApplicationCF.ENTITY_PROPERTIES,
				timestampUuid.toString(),
				ConversionUtils.bytebuffer("uuid"),
				ConversionUtils.bytebuffer(timestampUuid),
				timestamp));
		
		
		
		/*
		 properties.put(PROPERTY_UUID, itemId);
		properties.put(PROPERTY_TYPE,
				Schema.normalizeEntityType(entityType, false));
				properties.put(PROPERTY_TIMESTAMP, timestamp / 1000);
				properties.put(PROPERTY_CREATED, timestamp / 1000);
			properties.put(PROPERTY_MODIFIED, timestamp / 1000);
			
			.. for each property, batchSetProperty..
			   .. if property is unique:
			        Object key = createUniqueIndexKey(collectionName, propertyName, propertyValue);       
                    addInsertToMutator(m, ENTITY_UNIQUE, key, entityId, null, timestamp);
            ...if property is indexed: 
            relationManager.batchUpdatePropertyIndex
            // See definitions on User
            // index: username, email, name, firstname, lastname, middle
            
            add all properties to ENTITY_PROPERTIES: 
            HColumn<ByteBuffer, ByteBuffer> column = createColumn(
				bytebuffer(propertyName),
				serializeEntityProperty(entityType, propertyName, propertyValue),
				timestamp, be, be);
		       m.addInsertion(bytebuffer(key),
				ApplicationCF.ENTITY_PROPERTIES.toString(), column); 
             
		 */

		try {
			StorageProxy.mutate(mutations, ConsistencyLevel.QUORUM);
		} catch (Exception ex) {
			ex.printStackTrace();
			try {
				StorageProxy.mutate(mutations, ConsistencyLevel.ANY);
			} catch (Exception ex2) {
				ex2.printStackTrace();
			}
		}
		 
	}
}
