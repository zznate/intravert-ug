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
import io.teknek.intravert.util.ResponseUtil;

public class SetKeyspaceAction implements Action {

  @Override
  public void doAction(Operation o, Response response, RequestContext request,
          ApplicationContext application) {
    String name = (String) o.getArguments().get("name");
    request.getSession().setCurrentKeyspace(name);
    response.getResults().put(o.getId(), ResponseUtil.getDefaultHappy());
  }

}
