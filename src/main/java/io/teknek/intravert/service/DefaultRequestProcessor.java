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
      Operation operation = null;
      try {
        operation = request.getOperations().get(i);
        Action action = actionFatory.findAction(operation.getType());
        action.doAction(operation, response, requestContext, application);
      } catch (RuntimeException ex) {
        response.setExceptionId(operation.getId());
        response.setExceptionMessage(ex.getMessage());
        ex.printStackTrace();
        break;
      }
    }
  }

}
