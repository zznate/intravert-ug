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

import org.codehaus.jackson.JsonGenerationException;
import org.codehaus.jackson.map.JsonMappingException;
import org.junit.Test;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Maps;

public class CreateFilterTest extends BaseIntravertTest {

  @Test
  public void createApplicationFilter() throws JsonGenerationException, JsonMappingException, IllegalStateException, UnsupportedEncodingException, IOException, RuntimeException{
    Request request = new Request();
    Map<String,Object> filterDef = new HashMap<String,Object>();
    filterDef.put("spec", NitDesc.NitSpec.GROOVY_CLOSURE.toString());
    filterDef.put("name", "under21");
    filterDef.put("scope", "application");
    filterDef.put("script", "{ row -> if (row['value'].toInteger() > 21) return row else return null }");
    request.getOperations().add(new Operation()
    .withId("1").withType(ActionFactory.CREATE_FILTER).withArguments(filterDef));
    Client cl = new Client();
    Response response = cl.post("http://127.0.0.1:7654", request);
    List<Map> results = (List<Map>) response.getResults().get("1");
    Assert.assertEquals(new ImmutableMap.Builder<String, Object>().put("result", "ok").build(), results.get(0));
  }
}