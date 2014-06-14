package io.teknek.intravert.daemon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.client.Client;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;
import io.teknek.nit.NitDesc;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class UpsertTest extends BaseIntravertTest {

  @Test
  public void createApplicationFilter() throws JsonGenerationException, JsonMappingException, IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException{
    Request request = new Request();
    request.getOperations().add(new Operation().withId("0").withType(ActionFactory.CREATE_KEYSPACE).withArguments(
            new ImmutableMap.Builder<String, Object>()
            .put("name", "example")
            .put("replication", 1)
            .put("ignoreIfNotExists", true).build()));
    
    request.getOperations().add(new Operation().withId("1").withType(ActionFactory.CREATE_COLUMN_FAMILY).withArguments(
            new ImmutableMap.Builder<String, Object>()
            .put("keyspace", "example")
            .put("columnFamily", "upsert")
            .put("ignoreIfNotExists", true).build()));
    request.getOperations().add(new Operation()
    .withId("2").withType(ActionFactory.SET_KEYSPACE).withArguments(
            new ImmutableMap.Builder<String, Object>().put("name", "example").build()));
    request.getOperations().add(new Operation()
    .withId("3").withType(ActionFactory.UPSERT).withArguments(
            new ImmutableMap.Builder<String, Object>().put("rowkey", "ecapriolo")
            .put("column","firstname")
            .put("value","edward").build()));
    Client cl = new Client();
    Response response = cl.post("http://127.0.0.1:7654", request);
    List<Map> results = (List<Map>) response.getResults().get("1");
    Assert.assertEquals(new ImmutableMap.Builder<String, Object>().put("result", "ok").build(), results.get(0));
  }
}
/*
String keyspace = (String) operation.getArguments().get("keyspace");
String columnFamily = (String) operation.getArguments().get("columnfamily");
*/