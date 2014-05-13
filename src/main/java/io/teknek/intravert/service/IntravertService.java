package io.teknek.intravert.service;

import io.teknek.intravert.model.Request;
import io.teknek.intravert.model.Response;

public interface IntravertService {
  Response doRequest(Request request);
}
