package io.teknek.intravert.service;

import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

public interface RequestProcessor {
  void process(Request request, Response response, RequestContext requestContext, ApplicationContext application);
}
