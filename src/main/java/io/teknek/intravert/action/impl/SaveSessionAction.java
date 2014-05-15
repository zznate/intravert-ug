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

public class SaveSessionAction implements Action {

  @Override
  public void doAction(Operation operator, Response respose, RequestContext request,
          ApplicationContext application) {
    Map m = new HashMap();
    m.put(Constants.SESSION_ID, request.saveSession());
    respose.getResults().put(operator.getId(), Arrays.asList(m));
  }

}
