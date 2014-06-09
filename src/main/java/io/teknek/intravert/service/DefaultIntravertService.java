package io.teknek.intravert.service;

import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

public class DefaultIntravertService implements IntravertService {

  private RequestProcessor requestProcessor = new DefaultRequestProcessor();
  private ApplicationContext applicationContext = new ApplicationContext();
  
  @Override
  public Response doRequest(Request request) {
    Response response = new Response();
    RequestContext requestContext = new RequestContext();
    requestProcessor.process(request, response, requestContext, applicationContext);
    return response;
  }

}
