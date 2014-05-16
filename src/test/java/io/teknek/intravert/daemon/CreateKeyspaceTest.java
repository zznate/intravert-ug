package io.teknek.intravert.daemon;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.client.Client;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

import org.apache.http.impl.client.DefaultHttpClient;
import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CreateKeyspaceTest extends BaseIntravertTest {

  @Test
  public void createKeyspace() throws JsonGenerationException, JsonMappingException, IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException{
    Request request = new Request();
    request.getOperations().add(new Operation().withId("1").withType(ActionFactory.CREATE_SESSION));
    request.getOperations()
            .add(new Operation()
                     .withId("2")
                    .withType(ActionFactory.SET_KEYSPACE)
                    .withArguments(
                            new ImmutableMap.Builder<String, Object>().put("name", "bla").build()));
    
    DefaultHttpClient httpClient = new DefaultHttpClient();
    Assert.assertEquals(
            "{\"exceptionMessage\":null,\"exceptionId\":null,\"results\":{\"2\":[{\"result\":\"ok\"}],\"1\":[{\"session_id\":0}]},\"metaData\":{}}"
            , Client.postToURL("http://127.0.0.1:7654", new ObjectMapper().writeValueAsString(request), httpClient));
    
  }
}
