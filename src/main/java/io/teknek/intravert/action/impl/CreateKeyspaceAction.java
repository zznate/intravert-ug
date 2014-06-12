package io.teknek.intravert.action.impl;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.KsDef;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;

public class CreateKeyspaceAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    String keyspace = (String) operation.getArguments().get("name");
    int replication = (Integer) operation.getArguments().get("replication");
    createKeyspace(keyspace, replication);
  }

  public static void createKeyspace(String name, int replication){
    Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(0);
    KsDef def = new KsDef();
    def.setName(name);
    def.setStrategy_class("SimpleStrategy");
    Map<String, String> strat = new HashMap<String, String>();
    strat.put("replication_factor", Integer.toString(replication));
    def.setStrategy_options(strat);
    KSMetaData ksm = null;
    try {
        ksm = KSMetaData.fromThrift(def,
            cfDefs.toArray(new CFMetaData[cfDefs.size()]));
        MigrationManager.announceNewKeyspace(ksm);
    } catch (ConfigurationException e) {
        throw new RuntimeException(e);
    }
  }
}
