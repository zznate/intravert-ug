package io.teknek.intravert.daemon;

import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.client.Client;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.model.Type;
import io.teknek.nit.NitDesc;

import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Ignore;
import org.junit.Test;

import junit.framework.Assert;

public class SerializeTest {

  @Ignore
  public void test() throws JsonGenerationException, JsonMappingException, IOException{
    Request request = new Request();
    Map<String,Object> filterDef = new HashMap<String,Object>();
    filterDef.put("string", "under21");
    filterDef.put("bytes", new Type("Blob", "application".getBytes()));
    filterDef.put("abtype", new Type(5L));
    filterDef.put("int", 5);
    filterDef.put("long", new Type("Long", 5L));
    filterDef.put("comp", new Type("Composite(Long,String)", Arrays.asList(5l, "waa")));
    filterDef.put("comp", new Type("CompositeSep(Long,String)", Arrays.asList(5l, 0, "waa", 0)));
    request.getOperations().add(new Operation()
    .withId("1").withType(ActionFactory.CREATE_FILTER).withArguments(filterDef));
    ObjectMapper om = new ObjectMapper();
    Assert.assertEquals("", om.writeValueAsString(request));
    
    //Client cl = new Client();
    //Response response = cl.post("http://127.0.0.1:7654", request);
    //List<Map> results = (List<Map>) response.getResults().get("1");
    //Assert.assertEquals(new ImmutableMap.Builder<String, Object>().put("result", "ok").build(), results.get(0));
  }
  
}
