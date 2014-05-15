package io.teknek.intravert.service;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.action.ActionFactory;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

public class DefaultRequestProcessor implements RequestProcessor {

  private ActionFactory actionFatory = new ActionFactory();
  
  @Override
  public void process(Request request, Response response, RequestContext requestContext,
          ApplicationContext application) {
    for (int i = 0; i < request.getOperations().size(); i++) {
      Operation o = null;
      try {
        o = request.getOperations().get(i);
        Action action = actionFatory.findAction(o.getType());
        action.doAction(o, response, requestContext, application);
      } catch (RuntimeException ex) {
        response.setExceptionId(o.getId());
        response.setExceptionMessage(ex.getMessage());
        ex.printStackTrace();
        break;
      }
    }
  }

}
