package io.teknek.intravert.action.impl;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.action.filter.Filter;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.nit.NitDesc;
import io.teknek.nit.NitException;

public class CreateFilterAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    String name = (String) operation.getArguments().get("name");
    NitDesc n = new NitDesc();
    n.setSpec(NitDesc.NitSpec.valueOf((String) operation.getArguments().get("spec")));
    n.setTheClass((String) operation.getArguments().get("theClass"));
    n.setScript((String) operation.getArguments().get("script"));
    try {
      Filter f = io.teknek.nit.NitFactory.construct(n);
      request.getSession().putFilter(name, f);
    } catch (NitException e) {
      throw new RuntimeException(e);
    }
  }

}
