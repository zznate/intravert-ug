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

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.IntegerType;
import org.apache.cassandra.service.CassandraDaemon;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.usergrid.vx.server.IntravertDeamon;
import org.vertx.java.core.Vertx;

import static junit.framework.Assert.assertNotNull;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class IntraServiceTest {

  IntraService is = new IntraService();
  Vertx x = Vertx.newVertx();

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
		
		is.handleIntraReq(req, res, x);
		
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
		Assert.assertEquals( true , ((List<String>) res.getOpsRes().get(9)).contains("myks")  );
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
    is.handleIntraReq(req, res, x);
    Assert.assertEquals (  "OK" , res.getOpsRes().get(0)  );
    Assert.assertEquals( 1, res.getOpsRes().size() );
    Assert.assertNotNull( res.getException() );
    Assert.assertEquals( new Integer(1) , res.getExceptionId() );
  }
	
	 @Test
	  public void assumeTest() throws CharacterCodingException{
	    IntraReq req = new IntraReq();
	    req.add( IntraOp.setKeyspaceOp("assks") ); //0
	    req.add( IntraOp.createKsOp("assks", 1)); //1
	    req.add( IntraOp.createCfOp("asscf")); //2
	    req.add( IntraOp.setColumnFamilyOp("asscf") ); //3
	    req.add( IntraOp.setAutotimestampOp() ); //4
	    req.add( IntraOp.assumeOp("assks", "asscf", "value", "UTF-8"));//5
	    req.add( IntraOp.setOp("rowa", "col1", "wow")); //6
	    req.add( IntraOp.getOp("rowa", "col1")); //7
	    IntraRes res = new IntraRes();
	    is.handleIntraReq(req, res, x);
	    List<Map> x = (List<Map>) res.getOpsRes().get(7);
	    Assert.assertEquals( "wow",  x.get(0).get("value") );
	  }
	
	 @Test
	 public void filterTest() throws CharacterCodingException{
	   IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("filterks") ); //0
     req.add( IntraOp.createKsOp("filterks", 1)); //1
     req.add( IntraOp.createCfOp("filtercf")); //2
     req.add( IntraOp.setColumnFamilyOp("filtercf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("filterks", "filtercf", "value", "UTF-8"));//5
     req.add( IntraOp.setOp("rowa", "col1", "20")); //6
     req.add( IntraOp.setOp("rowa", "col2", "22")); //7
     req.add( IntraOp.createFilterOp("over21", "groovy",
     		"public class Over21 implements org.usergrid.vx.experimental.Filter { \n"+
        " public Map filter(Map row){ \n" +
        "   if (Integer.parseInt( row.get(\"value\") ) >21){ \n"+
        "     return row; \n" +
        "   } else { return null; } \n" +
        " } \n" +
        "} \n"
     )); //8
     req.add( IntraOp.filterModeOp("over21", true)); //9
     req.add( IntraOp.sliceOp("rowa", "col1", "col3", 10)); //10
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     System.out.println ( res.getException() );
     List<Map> results = (List<Map>) res.getOpsRes().get(10);
     Assert.assertEquals( "22", results.get(0).get("value") );
     Assert.assertEquals(1, results.size());
     
	 }
	 
	 @Test
   public void processorTest() throws CharacterCodingException{
     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("procks") ); //0
     req.add( IntraOp.createKsOp("procks", 1)); //1
     req.add( IntraOp.createCfOp("proccf")); //2
     req.add( IntraOp.setColumnFamilyOp("proccf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("procks", "proccf", "value", "UTF-8"));//5
     req.add( IntraOp.setOp("rowa", "col1", "wow")); //6
     req.add( IntraOp.getOp("rowa", "col1")); //7
     req.add( IntraOp.createProcessorOp("capitalize", "groovy", 
         "public class Capitalize implements org.usergrid.vx.experimental.Processor { \n"+
         "  public List<Map> process(List<Map> input){" +
         "    List<Map> results = new ArrayList<HashMap>();"+
         "    for (Map row: input){" +
         "      Map newRow = new HashMap(); "+
         "      newRow.put(\"value\",row.get(\"value\").toString().toUpperCase());" +
         "      results.add(newRow); "+
         "    } \n" +
         "    return results;"+
         "  }"+
         "}\n"
     ));//8
     //TAKE THE RESULT OF STEP 7 AND APPLY THE PROCESSOR TO IT
     req.add( IntraOp.processOp("capitalize", new HashMap(), 7));//9
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     List<Map> x = (List<Map>) res.getOpsRes().get(7);
     Assert.assertEquals( "wow",  x.get(0).get("value") );
     System.out.println(res.getException() );
     Assert.assertNull( res.getException() );
     x = (List<Map>) res.getOpsRes().get(9);
     Assert.assertEquals( "WOW",  x.get(0).get("value") );
   }
	 
	 
	 @Test
   public void intTest() throws CharacterCodingException{
     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("intks") ); //0
     req.add( IntraOp.createKsOp("intks", 1)); //1
     req.add( IntraOp.createCfOp("intcf")); //2
     req.add( IntraOp.setColumnFamilyOp("intcf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("intks", "intcf", "value", "UTF-8"));//5
     req.add( IntraOp.assumeOp("intks", "intcf", "column", "int32"));//6
     req.add( IntraOp.setOp("rowa", 1, "wow")); //7
     req.add( IntraOp.getOp("rowa", 1)); //8
     
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     List<Map> x = (List<Map>) res.getOpsRes().get(8);
     
     Assert.assertEquals( "wow",  x.get(0).get("value") );
     Assert.assertEquals( 1,  x.get(0).get("name") );
   }
	 
	 @Test
   public void compositeTest() throws CharacterCodingException{ 
     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("compks") ); //0
     req.add( IntraOp.createKsOp("compks", 1)); //1
     req.add( IntraOp.createCfOp("compcf")); //2
     req.add( IntraOp.setColumnFamilyOp("compcf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("compks", "compcf", "value", "CompositeType(UTF-8,int32)"));//5
     req.add( IntraOp.assumeOp("compks", "compcf", "column", "int32"));//6
     req.add( IntraOp.setOp("rowa", 1, new Object[] {"yo",0, 2,0})); //7
     req.add( IntraOp.getOp("rowa", 1)); //8
      
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     List<Map> x = (List<Map>) res.getOpsRes().get(8);
     Assert.assertEquals( 1,  x.get(0).get("name") );
     Assert.assertEquals( "yo",  ((Object [])x.get(0).get("value"))[0] );
     Assert.assertEquals( 2,  ((Object [])x.get(0).get("value"))[1] );
   }
	    
	 
	 @Test
   public void CqlTest() throws CharacterCodingException{ 
     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("cqlks") ); //0
     req.add( IntraOp.createKsOp("cqlks", 1)); //1
     req.add( IntraOp.createCfOp("cqlcf")); //2
     req.add( IntraOp.setColumnFamilyOp("cqlcf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("cqlks", "cqlcf", "value", "int32"));//5
     req.add( IntraOp.assumeOp("cqlks", "cqlcf", "column", "int32"));//6
     req.add( IntraOp.setOp("rowa", 1, 2)); //7
     req.add( IntraOp.getOp("rowa", 1)); //8
     req.add( IntraOp.cqlQuery("select * from cqlcf", "3.0.0"));//9
      
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     List<Map> x = (List<Map>) res.getOpsRes().get(8);

     Assert.assertEquals( 1,  x.get(0).get("name") );
     Assert.assertEquals( 2,  x.get(0).get("value") );
     x = (List<Map>) res.getOpsRes().get(9);
     //Assert.assertEquals( 2, ((ByteBuffer)x.get(2).get("value")).getInt() );
     Assert.assertEquals( new Integer(2), Int32Type.instance.compose((ByteBuffer)x.get(2).get("value")) );
     
   }

	 
	 @Test
   public void clearTest() throws CharacterCodingException{
     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("clearks") ); //0
     req.add( IntraOp.createKsOp("clearks", 1)); //1
     req.add( IntraOp.createCfOp("clearcf")); //2
     req.add( IntraOp.setColumnFamilyOp("clearcf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("clearks", "clearcf", "value", "UTF-8")); //5
     req.add( IntraOp.setOp("rowa", 1, "wow")); //6
     req.add( IntraOp.getOp("rowa", 1)); //7
     req.add( IntraOp.getOp("rowa", 1)); //8
     req.add( IntraOp.clear(8)); //9
     
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     
     List<Map> x = (List<Map>) res.getOpsRes().get(7);
     Assert.assertEquals( "wow",  x.get(0).get("value") );
     
     x = (List<Map>) res.getOpsRes().get(8);
     Assert.assertEquals(0, x.size());
   }

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void cqlEngineTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("myks") ); //0
    req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
    req.add( IntraOp.setAutotimestampOp() ); //2
    req.add( IntraOp.setOp("rowa", "col1", "7")); //3
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);

    ThriftClientState tcs = new ThriftClientState();
    tcs.setKeyspace("myks");
    ResultMessage rm = QueryProcessor.process("select * from mycf", ConsistencyLevel.ONE,tcs.getQueryState());
    CqlResult cr = rm.toThriftResult();
    List<Column> cols = cr.getRows().get(0).getColumns();
    String col0 = ByteBufferUtil.string(cols.get(0).bufferForName());
    String col1 = ByteBufferUtil.string(cols.get(1).bufferForName());
    String col2 = ByteBufferUtil.string(cols.get(2).bufferForName());
    String val0 = ByteBufferUtil.string(cols.get(0).bufferForValue());
    String val1 = ByteBufferUtil.string(cols.get(1).bufferForValue());
    String val2 = ByteBufferUtil.string(cols.get(2).bufferForValue());
    assertNotNull(rm);
  }
  
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void multiProcessTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("myks") ); //0
    req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
    req.add( IntraOp.setAutotimestampOp() ); //2
    req.add( IntraOp.assumeOp("myks", "mycf", "value", "UTF-8")); //3
    req.add( IntraOp.setOp("rowzz", "col1", "7")); //4
    req.add( IntraOp.setOp("rowzz", "col2", "8")); //5
    req.add( IntraOp.setOp("rowyy", "col4", "9")); //6
    req.add( IntraOp.setOp("rowyy", "col2", "7")); //7
    req.add( IntraOp.sliceOp("rowzz", "a", "z", 100));//8
    req.add( IntraOp.sliceOp("rowyy", "a", "z", 100));//9
    
    req.add( IntraOp.createMultiProcess("union", "groovy", 
    "public class Union implements org.usergrid.vx.experimental.MultiProcessor { \n"+
    "  public List<Map> multiProcess(Map<Integer,Object> results, Map params){ \n"+
    "    java.util.HashMap s = new java.util.HashMap(); \n"+
    "    List<Integer> ids = (List<Integer>) params.get(\"steps\");\n"+
    "    for (Integer id: ids) { \n"+
    "      List<Map> rows = results.get(id); \n"+
    "      for (Map row: rows){ \n"+
    "        s.put(row.get(\"value\"),\"\"); \n"+
    "      } \n"+
    "    } \n"+ 
    "    List<HashMap> ret = new ArrayList<HashMap>(); \n"+
    "    ret.add(s) \n"+
    "    return ret; \n" +
    "  } \n"+
    "} \n" )); //10 
    Map paramsMap = new HashMap();
    List<Integer> steps = new ArrayList<Integer>();
    steps.add(8);
    steps.add(9);
    paramsMap.put("steps", steps);
    req.add( IntraOp.multiProcess("union", paramsMap)); //11
    
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);

    List<Map> x = (List<Map>) res.getOpsRes().get(11);
    
    Set<String> expectedResults = new HashSet<String>();
    expectedResults.addAll( Arrays.asList(new String[] { "7", "8", "9"}));
    Assert.assertEquals(expectedResults, x.get(0).keySet());
    
  }
  
  
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void batchSetTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("myks") ); //0
    req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
    req.add( IntraOp.setAutotimestampOp() ); //2
    req.add( IntraOp.assumeOp("myks", "mycf", "value", "UTF-8")); //3
    req.add( IntraOp.assumeOp("myks", "mycf", "column", "UTF-8")); //4
    Map row1 = new HashMap();
    row1.put("rowkey", "batchkeya");
    row1.put("name", "col1");
    row1.put("value", "val1");
    
    Map row2 = new HashMap();
    row2.put("rowkey", "batchkeya");
    row2.put("name", "col2");
    row2.put("value", "val2");

    List<Map> rows = new ArrayList<Map>();
    rows.add(row1);
    rows.add(row2);
    req.add( IntraOp.batchSetOp(rows));//5
    req.add( IntraOp.sliceOp("batchkeya", "a", "z", 100));//6
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);
    List<Map> x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals(2, x.size());
    Assert.assertEquals("val1", x.get(0).get("value"));
    Assert.assertEquals("val2", x.get(1).get("value"));
    
  }
}
