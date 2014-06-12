package io.teknek.intravert.action.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;

import groovy.lang.Closure;
import io.teknek.intravert.action.Action;
import io.teknek.intravert.action.filter.Filter;
import io.teknek.intravert.action.filter.FilterFactory;
import io.teknek.intravert.model.Constants;
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
    String scope = (String) operation.getArguments().get("scope");
    NitDesc n = new NitDesc();
    n.setSpec(NitDesc.NitSpec.valueOf((String) operation.getArguments().get("spec")));
    n.setTheClass((String) operation.getArguments().get("theClass"));
    n.setScript((String) operation.getArguments().get("script"));
    try {
      Filter f = FilterFactory.createFilter(io.teknek.nit.NitFactory.construct(n));
      if ("session".equalsIgnoreCase(scope)){
        request.getSession().putFilter(name, f);
      } else if ("application".equalsIgnoreCase(scope)){
        addFilterToCluster(f, n);
        application.putFilter(name, f);
      }
    } catch (NitException | WriteTimeoutException | InvalidRequestException | ConfigurationException | UnavailableException | OverloadedException e) {
      throw new RuntimeException(e);
    }
    Map m = new HashMap();
    m.put(Constants.RESULT, Constants.OK);
    response.getResults().put(operation.getId(), Arrays.asList(m));
  }

  
  public void maybeCreateColumnFamily() throws InvalidRequestException, ConfigurationException {
    List<String> keyspaces = Schema.instance.getNonSystemTables();
    if (!keyspaces.contains("intravert")){
      CreateKeyspaceAction.createKeyspace("intravert", 1);
      CFMetaData cfm = null;
      CfDef def = new CfDef();
      def.setKeyspace("intravert");
      def.setName("filter");
      cfm = CFMetaData.fromThrift(def);
      cfm.addDefaultIndexNames();
      MigrationManager.announceNewColumnFamily(cfm);
    }
  }
  
  public void addFilterToCluster(Filter f, NitDesc n) throws InvalidRequestException, ConfigurationException, WriteTimeoutException, UnavailableException, OverloadedException{
    maybeCreateColumnFamily();
    List<IMutation> changes = new ArrayList<>();
    StorageProxy.mutate(changes, ConsistencyLevel.QUORUM);
    
  }
}
