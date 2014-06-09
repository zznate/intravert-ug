package io.teknek.intravert.action;

import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;

public interface Action {
  void doAction(Operation operation, Response response, RequestContext request, ApplicationContext application);
}
