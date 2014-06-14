package io.teknek.intravert.action.impl;

import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.action.filter.Filter;
import io.teknek.intravert.action.filter.FilterFactory;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.intravert.util.TypeUtil;

public class UpsertAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    
    Object rowkey =  TypeUtil.convert(operation.getArguments().get("rowkey"));
    Object column =  TypeUtil.convert(operation.getArguments().get("column"));
    Object value =  TypeUtil.convert(operation.getArguments().get("value"));
    
    Map m = new HashMap();
    m.put(Constants.RESULT, Constants.OK);
    response.getResults().put(operation.getId(), Arrays.asList(m));
    
  }

}
