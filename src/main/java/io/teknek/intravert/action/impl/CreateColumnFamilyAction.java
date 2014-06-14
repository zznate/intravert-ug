package io.teknek.intravert.action.impl;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.config.Schema;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.InvalidRequestException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.KsDef;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.model.Constants;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.intravert.util.ResponseUtil;

public class CreateColumnFamilyAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    String keyspace = (String) operation.getArguments().get("keyspace");
    String columnFamily = (String) operation.getArguments().get("columnFamily");
    boolean ignoreIfNotExists = (Boolean) operation.getArguments().get("ignoreIfNotExists");
    if (!Schema.instance.getNonSystemTables().contains(keyspace)){
      throw new RuntimeException("keyspace "+ keyspace + " does not exist");
    }
    if (false){
      if (ignoreIfNotExists){
        response.getResults().put(operation.getId(), ResponseUtil.getDefaultHappy());
        return;
      } else {
        throw new RuntimeException (keyspace +" already exists");
      }
    }
    try {
      createColumnfamily(keyspace, columnFamily);
    } catch (InvalidRequestException | ConfigurationException e) {
      throw new RuntimeException(e);
    }
    response.getResults().put(operation.getId(), ResponseUtil.getDefaultHappy());
  }

  public static void createColumnfamily(String keyspace, String columnFamily) throws InvalidRequestException, ConfigurationException{
    CFMetaData cfm = null;
    CfDef def = new CfDef();
    def.unsetId();
    def.setKeyspace(keyspace);
    def.setName(columnFamily);
    cfm = CFMetaData.fromThrift(def);
    cfm.addDefaultIndexNames();
    MigrationManager.announceNewColumnFamily(cfm);
  }
}
