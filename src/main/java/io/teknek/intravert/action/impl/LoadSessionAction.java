package io.teknek.intravert.action.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.intravert.service.Session;

public class LoadSessionAction implements Action {

  @Override
  public void doAction(Operation operator, Response response, RequestContext request,
          ApplicationContext application) {
    Long l = (Long) operator.getArguments().get(Constants.SESSION_ID);
    Session back = request.recoverSession(l);
    Map m = new HashMap();
    if (back != null){
      m.put(Constants.STATUS, Constants.OK);
      response.getResults().put(operator.getId(), Arrays.asList(m));
    } else {
      throw new RuntimeException("failed to recover session");
    }
  }

}
