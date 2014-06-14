package io.teknek.intravert.action.impl;

import java.util.ArrayList;
import java.util.List;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.intravert.util.ResponseUtil;
import io.teknek.intravert.util.TypeUtil;

public class UpsertAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    String keyspace =  (String) operation.getArguments().get("keyspace");
    String columnFamily =  (String) operation.getArguments().get("columnFamily");
    Object rowkey =  TypeUtil.convert(operation.getArguments().get("rowkey"));
    Object column =  TypeUtil.convert(operation.getArguments().get("column"));
    Object value =  TypeUtil.convert(operation.getArguments().get("value"));
    List<IMutation> changes = new ArrayList<>();
    RowMutation rm = new RowMutation(keyspace, ByteBufferUtil.bytes((String)rowkey));
    QueryPath qp = new QueryPath(columnFamily, null, ByteBufferUtil.bytes((String) column));
    rm.add(qp, ByteBufferUtil.bytes((String) value), System.nanoTime());
    changes.add(rm);
    try {
      StorageProxy.mutate(changes, ConsistencyLevel.QUORUM);
    } catch (WriteTimeoutException | UnavailableException | OverloadedException e) {
      throw new RuntimeException(e);
    }
    response.getResults().put(operation.getId(), ResponseUtil.getDefaultHappy());
    
  }

}

/*
 *  List<IMutation> changes = new ArrayList<>();
    {
      RowMutation rm = new RowMutation(IV_KEYSPACE, ByteBufferUtil.bytes(name));
      QueryPath qp = new QueryPath(FILTER_CF, null, ByteBufferUtil.bytes("spec"));
      rm.add(qp, ByteBufferUtil.bytes(n.getSpec().toString()), System.nanoTime());
      changes.add(rm);
    }
    {
      RowMutation rm = new RowMutation(IV_KEYSPACE, ByteBufferUtil.bytes(name));
      QueryPath qp = new QueryPath(FILTER_CF, null, ByteBufferUtil.bytes("theClass"));
      String cs = n.getTheClass() == null ? "" : n.getTheClass();
      rm.add(qp, ByteBufferUtil.bytes(cs), System.nanoTime());
      changes.add(rm);
    }
    {
      RowMutation rm = new RowMutation(IV_KEYSPACE, ByteBufferUtil.bytes(name));
      QueryPath qp = new QueryPath(FILTER_CF, null, ByteBufferUtil.bytes("script"));
      String cs = n.getScript() == null ? "" : n.getScript();
      rm.add(qp, ByteBufferUtil.bytes(cs), System.nanoTime());
      changes.add(rm);
    }    
    StorageProxy.mutate(changes, ConsistencyLevel.QUORUM);
    */
