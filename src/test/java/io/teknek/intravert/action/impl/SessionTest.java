package io.teknek.intravert.action.impl;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.DefaultIntravertService;
import io.teknek.intravert.service.IntravertService;
import io.teknek.intravert.test.TestUtils;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class SessionTest {

  @Test
  public void aTest(){
    IntravertService service = new DefaultIntravertService();
    String keyspaceName = "bla";
    {
      Request request = new Request();
      request.getOperations().add(new Operation().withId("1").withType(ActionFactory.CREATE_SESSION));
      request.getOperations()
              .add(new Operation()
                      .withId("2")
                      .withType(ActionFactory.SET_KEYSPACE)
                      .withArguments(
                              new ImmutableMap.Builder<String, Object>().put("name", keyspaceName).build()));
      Response response = service.doRequest(request);
      TestUtils.assertResponseDidNotFail(response);
      List<Map> results = (List<Map>) response.getResults().get("1");
      Assert.assertNotNull(results.get(0).get(Constants.SESSION_ID));
    }
    {
      Request other = new Request();
      other.getOperations().add(
              new Operation()
                      .withId("1")
                      .withType(ActionFactory.LOAD_SESSION)
                      .withArguments(
                              new ImmutableMap.Builder<String, Object>().put(Constants.SESSION_ID,
                                      0L).build()));
      other.getOperations().add(new Operation().withId("2").withType(ActionFactory.GET_KEYSPACE));
      Response second = service.doRequest(other);
      TestUtils.assertResponseDidNotFail(second);
      List<Map> results = (List<Map>) second.getResults().get("2");
      Assert.assertEquals(keyspaceName, results.get(0).get("keyspace"));
    }
  }
  
}
