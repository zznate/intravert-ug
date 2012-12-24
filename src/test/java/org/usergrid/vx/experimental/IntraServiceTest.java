package org.usergrid.vx.experimental;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.service.CassandraDaemon;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.usergrid.vx.server.IntravertDaemon;


public class IntraServiceTest  {

	static IntraService is;
	
	@BeforeClass
	public static void before(){
		deleteRecursive(new File ("/tmp/intra_cache"));
	    deleteRecursive(new File ("/tmp/intra_data"));
	    deleteRecursive(new File ("/tmp/intra_log"));
		System.setProperty("cassandra-foreground", "true");
	    System.setProperty("log4j.defaultInitOverride","true");
	    System.setProperty("log4j.configuration", "log4j.properties");
	    CassandraDaemon.initLog4j();
	    IntravertDaemon ucd = new IntravertDaemon();
		ucd.activate();
	    is = new IntraService();
	}
	
	@Test
	public void atest(){
		 
		IntraReq req = new IntraReq();
		req.add( IntraOp.setKeyspaceOp("myks") );
		req.add( IntraOp.createKsOp("myks", 1));
		req.add( IntraOp.createCfOp("mycf"));
		req.add( IntraOp.setColumnFamilyOp("mycf") );
		req.add( IntraOp.setAutotimestampOp() );
		req.add( IntraOp.setOp("5", "6", "7"));
		req.add( IntraOp.sliceOp("5", "1", "9", 4));
		req.add( IntraOp.getOp("5", "6"));
		//create a rowkey "9" with a column "10" and a value of the result
		//of operation 7
		//req.add( IntraOp.setOp("9", "10", IntraOp.getResRefOp(7, "value")));
		IntraRes res = new IntraRes();
		
		is.handleIntraReq(req, res);
		
		Assert.assertEquals (  "OK" , res.getOpsRes().get(0)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(1)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(2)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(3)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(4)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(5)  );
		//ToDO this should return something
		ArrayList<HashMap> sliceResults = new ArrayList<HashMap>() ;
		HashMap sliceExpected = new HashMap();
		sliceExpected.put("column", "6");
		sliceExpected.put("value", "7");
		sliceResults.add(sliceExpected);
		Assert.assertEquals (  sliceResults , res.getOpsRes().get(6)  );
		Assert.assertEquals ( sliceResults , res.getOpsRes().get(7) );
		
	}
	
    
    public static boolean deleteRecursive(File path) {
        if (!path.exists()) 
        	return false;
        boolean ret = true;
        if (path.isDirectory()){
            for (File f : path.listFiles()){
                ret = ret && deleteRecursive(f);
            }
        }
        return ret && path.delete();
    }
    /*
    /tmp/intra_cache
    /tmp/intra_data
    /tmp/intra_log
    */
}
