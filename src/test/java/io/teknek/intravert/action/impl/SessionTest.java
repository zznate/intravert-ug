package io.teknek.intravert.action.impl;

import java.util.List;
import java.util.Map;

import junit.framework.Assert;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.DefaultIntravertService;
import io.teknek.intravert.service.IntravertService;

import org.junit.Test;

public class SessionTest {

  @Test
  public void aTest(){
    IntravertService service = new DefaultIntravertService();
    Request request = new Request();
    request.getOperations().add(persist());
    Response response = service.doRequest(request);
    Assert.assertNull(response.getExceptionMessage());
    Assert.assertNull(response.getExceptionId());
    List<Map> results = (List<Map>) response.getResults().get("1");
    Assert.assertEquals(0L, results.get(0).get(Constants.SESSION_ID));
  }
  
  private Operation persist(){
    Operation o = new Operation();
    o.setId("1");
    o.setType("createsession");
    return o;
  }
}
