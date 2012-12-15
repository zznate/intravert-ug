package org.usergrid.vx.experimental;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.IMutation;
import org.apache.cassandra.db.ReadCommand;
import org.apache.cassandra.db.RowMutation;

import org.apache.cassandra.db.SliceByNamesReadCommand;
import org.apache.cassandra.db.SliceFromReadCommand;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.IsBootstrappingException;
import org.apache.cassandra.exceptions.OverloadedException;
import org.apache.cassandra.exceptions.ReadTimeoutException;
import org.apache.cassandra.exceptions.WriteTimeoutException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CassandraServer;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.thrift.ThriftValidation;
import org.apache.cassandra.thrift.TimedOutException;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IntraService {

	static CassandraServer thrift = new CassandraServer();
	public IntraRes handleIntraReq(IntraReq req,IntraRes res){

		if ( verifyReq(req,res) == false ){
			return res;
		} else {
			executeReq(req, res);
		}		
		return res;
	}
	
	private boolean executeReq(IntraReq req, IntraRes res) {
		String currentKeyspace="";
		String currentColumnFamily="";
		boolean autoTimestamp= true;
		long nanotime = System.nanoTime();
		for (int i =0;i<req.getE().size();i++){
			IntraOp op = req.getE().get(i);
			if (op.getType().equals("setkeyspace")) {
				currentKeyspace = (String) op.getOp().get("keyspace");
				res.getOpsRes().put(i, "OK");
			} else if (op.getType().equals("createkeyspace")) {
				String ks = (String) op.getOp().get("name");
				KsDef def = new KsDef();
				def.setName(ks);
				def.setStrategy_class("SimpleStrategy");
				Map<String,String> strat = new HashMap<String,String>();
				strat.put("replication_factor", "1");
				def.setStrategy_options(strat);
				KSMetaData ksm = null;
				try {
					ksm = KSMetaData.fromThrift(def );
				} catch (ConfigurationException e1) {
					// TODO Auto-generated catch block
					e1.printStackTrace();
				}
				res.getOpsRes().put(i, "OK");
				try {
					MigrationManager.announceNewKeyspace(ksm);
				} catch (ConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				
			} else if (op.getType().equals("createcolumnfamily")) {
				String cf = (String) op.getOp().get("name");
				CfDef def = new CfDef();
				def.setName(cf);
				def.setKeyspace(currentKeyspace);
				def.unsetId();
				CFMetaData cfm = null;

				try {
					cfm = CFMetaData.fromThrift(def);
					cfm.addDefaultIndexNames();
				} catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
					e.printStackTrace();
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
				

				// cfDefs.add(cfm);

				try {
					MigrationManager.announceNewColumnFamily(cfm);
				} catch (ConfigurationException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
				res.getOpsRes().put(i, "OK");
				
			} else if (op.getType().equals("setcolumnfamily")){
				currentColumnFamily = (String) op.getOp().get("columnfamily");
				res.getOpsRes().put(i, "OK");
			} else if (op.getType().equals("autotimestamp")){
				autoTimestamp = true;
				res.getOpsRes().put(i, "OK");
			} else if (op.getType().equals("set")){
				RowMutation rm = new RowMutation(currentKeyspace,byteBufferForObject(op.getOp().get("rowkey")));
				QueryPath qp = new QueryPath(currentColumnFamily,null, byteBufferForObject(op.getOp().get("columnName")) );
				rm.add(qp, byteBufferForObject(op.getOp().get("value")), (Long) (autoTimestamp ? nanotime : op.getOp().get("timestamp")));
				Collection<RowMutation> col = new ArrayList<RowMutation>();
				try {
					StorageProxy.mutateAtomically(col, ConsistencyLevel.ONE);
					res.getOpsRes().put(i, "OK");
				} catch (WriteTimeoutException e) {
					res.getOpsRes().put(i, e.getMessage());
				} catch (org.apache.cassandra.exceptions.UnavailableException e) {
					res.getOpsRes().put(i, e.getMessage());
				} catch (OverloadedException e) {
					res.getOpsRes().put(i, e.getMessage());
				}
			} else if (op.getType().equals("slice")){
				ByteBuffer rowkey = byteBufferForObject(op.getOp().get("rowkey"));
				ByteBuffer start = byteBufferForObject(op.getOp().get("start"));
				ByteBuffer end = byteBufferForObject(op.getOp().get("end"));
				List<ReadCommand> commands = new ArrayList<ReadCommand>(1);
				QueryPath qp = new QueryPath(currentColumnFamily);
				commands.add(new SliceFromReadCommand(currentKeyspace, rowkey, qp, start, end, false, 100));
				try {
					StorageProxy.read(commands, ConsistencyLevel.ONE);
				} catch (ReadTimeoutException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (org.apache.cassandra.exceptions.UnavailableException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IsBootstrappingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				} catch (IOException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
		return true;
	}
	
	private ByteBuffer byteBufferForObject(Object o){
		if (o instanceof String){
			return ByteBufferUtil.bytes((String) o);
		} throw new RuntimeException( "can not serializer "+o);
	}
	
	public boolean verifyReq(IntraReq req, IntraRes res){
		if (req == null){
			res.setException("FATAL: REQUEST IS NULL");
			return false;
		}
		for (int i =0;i<req.getE().size();i++){
			IntraOp op = req.getE().get(i);
			if (op.getType()==null){
				res.setException("FATAL: op i had no type");
			} else if (op.getType().equals("setkeyspace")){
				
			} else if (op.getType().equals("setcolumnfamily")){
				
			} else if (op.getType().equals("autotimestamp")){
				
			} else if (op.getType().equals("set")){
			}
		}
		return true;
		
	}
	
   
}
