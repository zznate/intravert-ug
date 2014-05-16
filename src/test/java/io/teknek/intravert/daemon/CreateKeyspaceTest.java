package io.teknek.intravert.daemon;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

import org.junit.Test;

import com.google.common.collect.ImmutableMap;

public class CreateKeyspaceTest extends BaseIntravertTest {

  @Test
  public void createKeyspace(){
    Request request = new Request();
    request.getOperations().add(new Operation().withId("1").withType(ActionFactory.CREATE_SESSION));
    request.getOperations()
            .add(new Operation()
                    .withId("2")
                    .withType(ActionFactory.SET_KEYSPACE)
                    .withArguments(
                            new ImmutableMap.Builder<String, Object>().put("name", "bla").build()));
    
    Response response = intravert.intravertServer.getService().doRequest(request);
    List<Map> results = (List<Map>) response.getResults().get("1");
    Assert.assertEquals(0L, results.get(0).get(Constants.SESSION_ID));
  }
}
