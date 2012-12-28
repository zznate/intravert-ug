package org.usergrid.vx.experimental;

import java.io.File;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.usergrid.vx.server.IntravertDeamon;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class IntraServiceTest {

  IntraService is = new IntraService();

  @DataLoader(dataset = "mydata.txt")
  @Test
	public void atest() throws CharacterCodingException{
		IntraReq req = new IntraReq();
		req.add( IntraOp.setKeyspaceOp("myks") ); //0
		req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
		req.add( IntraOp.setAutotimestampOp() ); //2
		req.add( IntraOp.setOp("rowa", "col1", "7")); //3
		req.add( IntraOp.sliceOp("rowa", "col1", "z", 4)); //4
		req.add( IntraOp.getOp("rowa", "col1")); //5
		//create a rowkey "rowb" with a column "col2" and a value of the result of operation 7
		req.add( IntraOp.setOp("rowb", "col2", IntraOp.getResRefOp(5, "value"))); //6
		//Read this row back 
		req.add( IntraOp.getOp("rowb", "col2"));//7
		
		req.add( IntraOp.consistencyOp("ALL")); //8
		req.add( IntraOp.listKeyspacesOp()); //9
		req.add(IntraOp.listColumnFamilyOp("myks"));//10
		IntraRes res = new IntraRes();
		
		is.handleIntraReq(req, res);
		
		Assert.assertEquals (  "OK" , res.getOpsRes().get(0)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(1)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(2)  );
		Assert.assertEquals (  "OK" , res.getOpsRes().get(3)  );
		List<Map> x = (List<Map>) res.getOpsRes().get(4);
		Assert.assertEquals( "col1", ByteBufferUtil.string((ByteBuffer) x.get(0).get("name")) );
		Assert.assertEquals( "7", ByteBufferUtil.string((ByteBuffer) x.get(0).get("value")) );
		
		x = (List<Map>) res.getOpsRes().get(5);
		Assert.assertEquals( "7", ByteBufferUtil.string((ByteBuffer) x.get(0).get("value"))  );
		
		Assert.assertEquals( "OK" , res.getOpsRes().get(6)  );
		
		x = (List<Map>) res.getOpsRes().get(7);
		Assert.assertEquals( "7", ByteBufferUtil.string((ByteBuffer) x.get(0).get("value"))  );
		
		Assert.assertEquals( "OK" , res.getOpsRes().get(8)  );
		Assert.assertEquals( Arrays.asList("myks") , res.getOpsRes().get(9)  );
		Set s = new HashSet();
		s.add("mycf");
		Assert.assertEquals( s , res.getOpsRes().get(10)  );
		
	}
	
	@Test
  public void exceptionHandleTest() throws CharacterCodingException{
    IntraReq req = new IntraReq();
    req.add( IntraOp.createKsOp("makeksagain", 1)); //0
    req.add( IntraOp.createKsOp("makeksagain", 1)); //1
    req.add( IntraOp.createKsOp("makeksagain", 1)); //2
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res);
    Assert.assertEquals (  "OK" , res.getOpsRes().get(0)  );
    Assert.assertEquals( 1, res.getOpsRes().size() );
    Assert.assertNotNull( res.getException() );
    Assert.assertEquals( new Integer(1) , res.getExceptionId() );
  }	
}
