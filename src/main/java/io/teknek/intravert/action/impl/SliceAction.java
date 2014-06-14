package io.teknek.intravert.action.impl;

import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.ColumnFamily;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IColumn;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.Row;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.UnavailableException;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.utils.ByteBufferUtil;

import io.teknek.intravert.action.Action;
import io.teknek.intravert.model.Operation;
import io.teknek.intravert.model.Response;
import io.teknek.intravert.service.ApplicationContext;
import io.teknek.intravert.service.RequestContext;
import io.teknek.intravert.util.ResponseUtil;
import io.teknek.intravert.util.TypeUtil;

public class SliceAction implements Action {

  @Override
  public void doAction(Operation operation, Response response, RequestContext request,
          ApplicationContext application) {
    String keyspace =  (String) operation.getArguments().get("keyspace");
    String columnFamily =  (String) operation.getArguments().get("columnFamily");
    Object rowkey =  TypeUtil.convert(operation.getArguments().get("rowkey"));
    Object start =  TypeUtil.convert(operation.getArguments().get("start"));
    Object end =  TypeUtil.convert(operation.getArguments().get("end"));
    List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
    QueryPath path = new QueryPath(columnFamily, null);
    //TODO hardcode
    SliceFromReadCommand sr = new SliceFromReadCommand(keyspace,
            ByteBufferUtil.bytes((String)rowkey), path, ByteBufferUtil.bytes((String)start), ByteBufferUtil.bytes((String)end), false, 100);
    commands.add(sr);
    List<Row> results = null;
    List<Map> returnResults = new ArrayList<>();
    try {
      results = StorageProxy.read(commands, ConsistencyLevel.ONE);
    } catch (ReadTimeoutException | UnavailableException | IsBootstrappingException e) {
      throw new RuntimeException(e);
    }
    ColumnFamily cf = results.get(0).cf;
    if (cf == null) {
      response.getResults().put(operation.getId(), Arrays.asList(new HashMap()));
      return;
    }
    Iterator<IColumn> it = cf.iterator();
    while (it.hasNext()) {
      IColumn column = it.next();
      if (column.isLive()) {
        HashMap<String,Object> m = new HashMap<>(4);
        //TODO hard code
        try {
          m.put("name", ByteBufferUtil.string(column.name()));
          m.put("value", ByteBufferUtil.string(column.value()));
        } catch (CharacterCodingException e) {
          e.printStackTrace();
        }
        returnResults.add(m);
      }
    }
    response.getResults().put(operation.getId(), returnResults);
  }

  
}
/*
ByteBuffer rowkey = HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(rowKeyParam));
ByteBuffer start = HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(startParam));
ByteBuffer end = HandlerUtils.instance.byteBufferForObject(HandlerUtils.instance.resolveObject(endParam));
List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
QueryPath path = new QueryPath(HandlerUtils.instance.determineCf(params, state, null), null);
SliceFromReadCommand sr = new SliceFromReadCommand(HandlerUtils.instance.determineKs(params, state, null),
        rowkey, path, start, end, false, 100);
commands.add(sr);
List<Row> results = null;
try {
  results = StorageProxy.read(commands, HandlerUtils.instance.determineConsistencyLevel(state));
  ColumnFamily cf = results.get(0).cf;
*/