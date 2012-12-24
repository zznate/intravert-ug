package org.usergrid.vx.experimental;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.usergrid.vx.server.IntravertDeamon;


public class IntraServiceTest  {

	static IntraService is;
  static IntravertDeamon intravertDeamon = new IntravertDeamon();

  @BeforeClass
  public static void before(){
    deleteRecursive(new File ("/tmp/intra_cache"));
    deleteRecursive(new File ("/tmp/intra_data"));
    deleteRecursive(new File ("/tmp/intra_log"));
    System.setProperty("cassandra-foreground", "true");
    System.setProperty("log4j.defaultInitOverride","true");
    System.setProperty("log4j.configuration", "log4j.properties");
    intravertDeamon.activate();
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
		req.add( IntraOp.setOp("key1", "col1", "val1"));
		req.add( IntraOp.sliceOp("key1", Character.MIN_VALUE + "", Character.MAX_VALUE + "", 4));
		req.add( IntraOp.getOp("mykey", "col1"));
		IntraRes res = new IntraRes();
		
		is.handleIntraReq(req, res);
		
		Assert.assertEquals (  "OK" , res.getOpsRes().get(0)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(1)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(2)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(3)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(4)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(5)  );
		//ToDO this should return something

		Assert.assertEquals(ByteBufferUtil.bytes("val1"), ((Map)((List) res.getOpsRes().get(6)).get(0)).get("value"));
		//Assert.assertEquals ( sliceResults , res.getOpsRes().get(7) );
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
