package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeoutException;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.ConfigurationException;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.RowMutation;
import org.apache.cassandra.db.filter.QueryPath;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.InvalidRequestException;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.UnavailableException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class IntraService {

	public IntraRes handleIntraReq(IntraReq req,IntraRes res){
		if ( verifyReq(req,res) == false ){
			return res;
		} else {
			executeReq(req, res);
		}		
		return res;
	}
	
	private boolean executeReq(IntraReq req, IntraRes res){
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
					res.getOpsRes().put(i, "OK");
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
				try {
					MigrationManager.announceNewKeyspace(ksm);
				} catch (ConfigurationException e) {
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
				} catch (InvalidRequestException e) {
					e.printStackTrace();
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
                try {
					cfm.addDefaultIndexNames();
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
                //cfDefs.add(cfm);
				try {
					MigrationManager.announceNewColumnFamily(cfm);
					res.getOpsRes().put(i, "OK");
				} catch (ConfigurationException e) {
					e.printStackTrace();
				}
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
				try {
					StorageProxy.mutate(Arrays.asList(rm), ConsistencyLevel.ANY);
					res.getOpsRes().put(i, "OK");
				} catch (UnavailableException e) {
					res.getOpsRes().put(i, e.getMessage());
				} catch (TimeoutException e) {
					res.getOpsRes().put(i, e.getMessage());
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
