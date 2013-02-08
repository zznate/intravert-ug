package org.usergrid.vx.experimental;

import static junit.framework.Assert.assertNotNull;

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

import com.google.common.collect.ImmutableMap;

import org.apache.cassandra.cql3.QueryProcessor;
import org.apache.cassandra.db.ConsistencyLevel;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.CqlResult;
import org.apache.cassandra.thrift.ThriftClientState;
import org.apache.cassandra.transport.messages.ResultMessage;
import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.vertx.java.core.Vertx;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class IntraServiceITest {

  IntraService is = new IntraService();
  Vertx x = Vertx.newVertx();

  @DataLoader(dataset = "mydata.txt")
  @Test
	public void atest() throws CharacterCodingException{
		IntraReq req = new IntraReq();
		req.add( Operations.setKeyspaceOp("myks") ); //0
		req.add( Operations.setColumnFamilyOp("mycf") ); //1
		req.add( Operations.setAutotimestampOp() ); //2
		req.add( Operations.setOp("rowa", "col1", "7")); //3
		req.add( Operations.sliceOp("rowa", "col1", "z", 4)); //4
		req.add( Operations.getOp("rowa", "col1")); //5
		//create a rowkey "rowb" with a column "col2" and a value of the result of operation 7
                req.add( Operations.setOp("rowb", "col2", ImmutableMap.of(
                    "type", "GETREF",
                    "op", ImmutableMap.of("resultref", 5, "wanted", "value")
                ))); //6
		//Read this row back 
		req.add( Operations.getOp("rowb", "col2"));//7
		
		req.add( Operations.consistencyOp("ALL")); //8
		req.add( Operations.listKeyspacesOp()); //9
		req.add(Operations.listColumnFamilyOp("myks"));//10
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
    req.add( Operations.createKsOp("makeksagain", 1)); //0
    req.add( Operations.createKsOp("makeksagain", 1)); //1
    req.add( Operations.createKsOp("makeksagain", 1)); //2
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
	    req.add( Operations.setKeyspaceOp("assks") ); //0
	    req.add( Operations.createKsOp("assks", 1)); //1
	    req.add( Operations.createCfOp("asscf")); //2
	    req.add( Operations.setColumnFamilyOp("asscf") ); //3
	    req.add( Operations.setAutotimestampOp() ); //4
	    req.add( Operations.assumeOp("assks", "asscf", "value", "UTF-8"));//5
	    req.add( Operations.setOp("rowa", "col1", "wow")); //6
	    req.add( Operations.getOp("rowa", "col1")); //7
	    IntraRes res = new IntraRes();
	    is.handleIntraReq(req, res, x);
	    List<Map> x = (List<Map>) res.getOpsRes().get(7);
	    Assert.assertEquals( "wow",  x.get(0).get("value") );
	  }
	
	 @Test
	 public void filterTest() throws CharacterCodingException{
	   IntraReq req = new IntraReq();
     req.add( Operations.setKeyspaceOp("filterks") ); //0
     req.add( Operations.createKsOp("filterks", 1)); //1
     req.add( Operations.createCfOp("filtercf")); //2
     req.add( Operations.setColumnFamilyOp("filtercf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("filterks", "filtercf", "value", "UTF-8"));//5
     req.add( Operations.setOp("rowa", "col1", "20")); //6
     req.add( Operations.setOp("rowa", "col2", "22")); //7
     req.add( Operations.createFilterOp("over21", "groovy",
     		"public class Over21 implements org.usergrid.vx.experimental.Filter { \n"+
        " public Map filter(Map row){ \n" +
        "   if (Integer.parseInt( row.get(\"value\") ) >21){ \n"+
        "     return row; \n" +
        "   } else { return null; } \n" +
        " } \n" +
        "} \n"
     )); //8
     req.add( Operations.filterModeOp("over21", true)); //9
     req.add( Operations.sliceOp("rowa", "col1", "col3", 10)); //10
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
     req.add( Operations.setKeyspaceOp("procks") ); //0
     req.add( Operations.createKsOp("procks", 1)); //1
     req.add( Operations.createCfOp("proccf")); //2
     req.add( Operations.setColumnFamilyOp("proccf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("procks", "proccf", "value", "UTF-8"));//5
     req.add( Operations.setOp("rowa", "col1", "wow")); //6
     req.add( Operations.getOp("rowa", "col1")); //7
     req.add( Operations.createProcessorOp("capitalize", "groovy", 
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
     req.add( Operations.processOp("capitalize", new HashMap(), 7));//9
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
     req.add( Operations.setKeyspaceOp("intks") ); //0
     req.add( Operations.createKsOp("intks", 1)); //1
     req.add( Operations.createCfOp("intcf")); //2
     req.add( Operations.setColumnFamilyOp("intcf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("intks", "intcf", "value", "UTF-8"));//5
     req.add( Operations.assumeOp("intks", "intcf", "column", "int32"));//6
     req.add( Operations.setOp("rowa", 1, "wow")); //7
     req.add( Operations.getOp("rowa", 1)); //8
     
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     List<Map> x = (List<Map>) res.getOpsRes().get(8);
     
     Assert.assertEquals( "wow",  x.get(0).get("value") );
     Assert.assertEquals( 1,  x.get(0).get("name") );
   }
	 @Test
	 public void ttlTest () {
	     IntraReq req = new IntraReq();
	     req.add( Operations.setKeyspaceOp("ttlks") ); //0
	     req.add( Operations.createKsOp("ttlks", 1)); //1
	     req.add( Operations.createCfOp("ttlcf")); //2
	     req.add( Operations.setColumnFamilyOp("ttlcf") ); //3
	     req.add( Operations.setAutotimestampOp() ); //4
	     req.add( Operations.assumeOp("ttlks", "ttlcf", "value", "UTF-8"));//5
	     req.add( Operations.assumeOp("ttlks", "ttlcf", "column", "int32"));//6
	     req.add( Operations.setOp("rowa", 1, "wow")); //7
	     req.add( Operations.setOp("rowa", 2, "wow").set("ttl", 1)); //8
	     //req.add( Operations.sliceOp("rowa", 1, 5, 4) ); //9
		 
	     IntraRes res = new IntraRes();
	     is.handleIntraReq(req, res, x);
	     Assert.assertEquals( "OK", res.getOpsRes().get(8));
	     try {
			Thread.sleep(2000);
		} catch (InterruptedException e) {
		}
	     
	    IntraReq r = new IntraReq();
	    r.add( Operations.setKeyspaceOp("ttlks") ); //0
	    r.add( Operations.setColumnFamilyOp("ttlcf") ); //1
	     r.add( Operations.assumeOp("ttlks", "ttlcf", "value", "UTF-8"));//2
	     r.add( Operations.assumeOp("ttlks", "ttlcf", "column", "int32"));//3
	    r.add( Operations.sliceOp("rowa", 1, 5, 4) ); //4
	    IntraRes rs = new IntraRes();
	    
	    is.handleIntraReq(r, rs, x);
	    
	    List<Map> x = (List<Map>) rs.getOpsRes().get(4);
	    System.out.println(x);
	    Assert.assertEquals(1, x.size());
	    
	 }
	 
	 @Test
   public void compositeTest() throws CharacterCodingException{ 
     IntraReq req = new IntraReq();
     req.add( Operations.setKeyspaceOp("compks") ); //0
     req.add( Operations.createKsOp("compks", 1)); //1
     req.add( Operations.createCfOp("compcf")); //2
     req.add( Operations.setColumnFamilyOp("compcf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("compks", "compcf", "value", "CompositeType(UTF-8,int32)"));//5
     req.add( Operations.assumeOp("compks", "compcf", "column", "int32"));//6
     req.add( Operations.setOp("rowa", 1, new Object[] {"yo",0, 2,0})); //7
     req.add( Operations.getOp("rowa", 1)); //8
      
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
     req.add( Operations.setKeyspaceOp("cqlks") ); //0
     req.add( Operations.createKsOp("cqlks", 1)); //1
     req.add( Operations.createCfOp("cqlcf")); //2
     req.add( Operations.setColumnFamilyOp("cqlcf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("cqlks", "cqlcf", "value", "int32"));//5
     req.add( Operations.assumeOp("cqlks", "cqlcf", "column", "int32"));//6
     req.add( Operations.setOp("rowa", 1, 2)); //7
     req.add( Operations.getOp("rowa", 1)); //8
     req.add( Operations.cqlQuery("select * from cqlcf", "3.0.0"));//9
      
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
	public void CqlNoResultTest() throws CharacterCodingException {
		IntraReq req = new IntraReq();
		req.add ( Operations.setKeyspaceOp("system") );
		req.add(Operations
				.cqlQuery(
						"CREATE KEYSPACE test WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}",
						"3.0.0"));// 0
		IntraRes res = new IntraRes();
		is.handleIntraReq(req, res, x);
		List<Map> x = (List<Map>) res.getOpsRes().get(1);
		Assert.assertEquals(2, res.getOpsRes().size());
	}
	 
	 @Test
   public void clearTest() throws CharacterCodingException{
     IntraReq req = new IntraReq();
     req.add( Operations.setKeyspaceOp("clearks") ); //0
     req.add( Operations.createKsOp("clearks", 1)); //1
     req.add( Operations.createCfOp("clearcf")); //2
     req.add( Operations.setColumnFamilyOp("clearcf") ); //3
     req.add( Operations.setAutotimestampOp() ); //4
     req.add( Operations.assumeOp("clearks", "clearcf", "value", "UTF-8")); //5
     req.add( Operations.setOp("rowa", 1, "wow")); //6
     req.add( Operations.getOp("rowa", 1)); //7
     req.add( Operations.getOp("rowa", 1)); //8
     req.add( Operations.clear(8)); //9
     
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);
     
     List<Map> x = (List<Map>) res.getOpsRes().get(7);
     Assert.assertEquals("wow", x.get(0).get("value"));
     
     x = (List<Map>) res.getOpsRes().get(8);
     Assert.assertEquals(0, x.size());
   }

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void cqlEngineTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add( Operations.setKeyspaceOp("myks") ); //0
    req.add( Operations.setColumnFamilyOp("mycf") ); //1
    req.add( Operations.setAutotimestampOp() ); //2
    req.add( Operations.setOp("rowa", "col1", "7")); //3
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
    req.add( Operations.setKeyspaceOp("myks") ); //0
    req.add( Operations.setColumnFamilyOp("mycf") ); //1
    req.add(Operations.setAutotimestampOp()); //2
    req.add(Operations.assumeOp("myks", "mycf", "value", "UTF-8")); //3
    req.add(Operations.setOp("rowzz", "col1", "7")); //4
    req.add(Operations.setOp("rowzz", "col2", "8")); //5
    req.add( Operations.setOp("rowyy", "col4", "9")); //6
    req.add( Operations.setOp("rowyy", "col2", "7")); //7
    req.add( Operations.sliceOp("rowzz", "a", "z", 100));//8
    req.add(Operations.sliceOp("rowyy", "a", "z", 100));//9
    
    req.add( Operations.createMultiProcess("union", "groovy",
            "public class Union implements org.usergrid.vx.experimental.MultiProcessor { \n" +
                    "  public List<Map> multiProcess(Map<Integer,Object> results, Map params){ \n" +
                    "    java.util.HashMap s = new java.util.HashMap(); \n" +
                    "    List<Integer> ids = (List<Integer>) params.get(\"steps\");\n" +
                    "    for (Integer id: ids) { \n" +
                    "      List<Map> rows = results.get(id); \n" +
                    "      for (Map row: rows){ \n" +
                    "        s.put(row.get(\"value\"),\"\"); \n" +
                    "      } \n" +
                    "    } \n" +
                    "    List<HashMap> ret = new ArrayList<HashMap>(); \n" +
                    "    ret.add(s) \n" +
                    "    return ret; \n" +
                    "  } \n" +
                    "} \n")); //10
    Map paramsMap = new HashMap();
    List<Integer> steps = new ArrayList<Integer>();
    steps.add(8);
    steps.add(9);
    paramsMap.put("steps", steps);
    req.add( Operations.multiProcess("union", paramsMap)); //11
    
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
    req.add( Operations.setKeyspaceOp("myks") ); //0
    req.add( Operations.setColumnFamilyOp("mycf") ); //1
    req.add( Operations.setAutotimestampOp() ); //2
    req.add( Operations.assumeOp("myks", "mycf", "value", "UTF-8")); //3
    req.add( Operations.assumeOp("myks", "mycf", "column", "UTF-8")); //4
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
    req.add( Operations.batchSetOp(rows));//5
    req.add(Operations.sliceOp("batchkeya", "a", "z", 100));//6
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);
    List<Map> x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals(2, x.size());
    Assert.assertEquals("val1", x.get(0).get("value"));
    Assert.assertEquals("val2", x.get(1).get("value"));
    
  }
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void serviceProcessorTest() throws Exception {
	  IntraReq req = new IntraReq();
	  req.add( Operations.createServiceProcess("buildMySecondary", "groovy",
	  		"import org.usergrid.vx.experimental.* \n"+
			"public class MyBuilder extends TwoExBuilder { \n"+
	  		"   \n"+
			"} \n" ) ); //0
	  Map reqObj = new HashMap();
	  reqObj.put("userid", "bsmith");
	  reqObj.put("fname", "bob");
	  reqObj.put("lname", "smith");
	  reqObj.put("city", "NYC");
		
	  req.add( Operations.setKeyspaceOp("myks") );//1
	  req.add( Operations.setAutotimestampOp() );//2
	  req.add( Operations.createCfOp("users") );//3
	  req.add(Operations.createCfOp("usersbycity"));//4
	  req.add( Operations.createCfOp("usersbylast") );//5
	  req.add(Operations.serviceProcess("buildMySecondary", reqObj));//6
	  req.add( Operations.setColumnFamilyOp("usersbycity")); //7
	  req.add( Operations.sliceOp("NYC", "a", "z", 5)); //8
	  IntraRes res = new IntraRes();
	  is.handleIntraReq(req, res, x);
	  List<Map> r = (List<Map>) res.getOpsRes().get(8);
	  Assert.assertEquals("bsmith", ByteBufferUtil.string((ByteBuffer) r.get(0).get("name")));
  }
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void jsonTest() throws Exception {
	  String array = "[{\"value\": 1},{\"value\": 2}, {\"value\": 3},{\"value\": 4}]";
    IntraReq req = new IntraReq();
    req.add( Operations.setKeyspaceOp("myks") ); //0
    req.add( Operations.setColumnFamilyOp("mycf") ); //1
    req.add( Operations.setAutotimestampOp() ); //2
    req.add( Operations.assumeOp("myks", "mycf", "value", "UTF-8")); //3
    req.add( Operations.assumeOp("myks", "mycf", "column", "UTF-8")); //4
    Map row1 = new HashMap();
    row1.put("rowkey", "jsonkey");
    row1.put("name", "data");
    row1.put("value", array);
    
    List<Map> rows = new ArrayList<Map>();
    rows.add(row1);

    req.add( Operations.batchSetOp(rows));//5
    req.add( Operations.sliceOp("jsonkey", "a", "z", 100));//6
    req.add( Operations.createProcessorOp("JsonPathEx", "groovy", 
            "import com.jayway.jsonpath.*; \n" +
            "public class JsonPathEx implements org.usergrid.vx.experimental.Processor { \n"+
            "  public List<Map> process(List<Map> input){" +
            "    List<Map> results = new ArrayList<HashMap>();"+
            "    for (Map row: input){" +
            "      Map newRow = new HashMap(); "+
            // grovvy requires you to escape $
            "      Integer match = JsonPath.read(row.get(\"value\").toString(), \"\\$.[1].value\"); \n"+
            "      newRow.put(\"value\",match.toString()); \n "+
            "      results.add(newRow); \n"+
            "    } \n" +
            "    return results;"+
            "  }"+
            "}\n"
        ));//7
    req.add( Operations.processOp("JsonPathEx", Collections.EMPTY_MAP, 6));//8

    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);
    List<Map> x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals(1, x.size());
    Assert.assertEquals("data", x.get(0).get("name"));
    Assert.assertEquals(array, x.get(0).get("value"));
    
    List<Map> y = (List<Map>) res.getOpsRes().get(8);
    Assert.assertEquals(1, y.size());
    Assert.assertEquals("2", y.get(0).get("value") );
  }
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void saveStateTest() throws Exception {
    //IntraClient ic = new IntraClient();
    //ic.setPayload("json");
    IntraReq r = new IntraReq();
    r.add( Operations.setKeyspaceOp("myks"));//0
    r.add( Operations.setColumnFamilyOp("mycf")); //1
    r.add( Operations.setAutotimestampOp() ); //2
    r.add( Operations.setOp("a", "b", "c") ); //3
    r.add( Operations.assumeOp("myks", "mycf", "value", "UTF-8") );//4
    r.add( Operations.saveState() );//5
    
    IntraRes res = new IntraRes();
    is.handleIntraReq(r, res, x);
    
    //IntraRes res = ic.sendBlocking(r);
    Assert.assertEquals("OK",res.getOpsRes().get(3) );
    int id = (Integer) res.getOpsRes().get(5);
    
    IntraReq r2 = new IntraReq();
    r2.add( Operations.restoreState(id));//0
    r2.add( Operations.setOp("d", "e", "f"));//1
    r2.add( Operations.getOp("d", "e")); //2
    IntraRes res2 = new IntraRes();
    is.handleIntraReq(r2, res2, x);
    Assert.assertEquals("f", ((List<Map>) res2.getOpsRes().get(2)).get(0).get("value") );
  }
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void loadTestClient() throws Exception{
    final int ops=1;
    long start = System.currentTimeMillis();
    
    Thread t = new Thread (){
       IntraClient ic = new IntraClient();
       
      public void run(){
        //ic.setPayload("json");
        ic.setPayload("jsonsmile");
        for (int i =0;i<ops;++i){
          IntraReq req = new IntraReq();
          req.add( Operations.setKeyspaceOp("myks") ); //0
          req.add( Operations.setColumnFamilyOp("mycf") ); //1
          req.add( Operations.setAutotimestampOp() ); //2
          req.add( Operations.setOp("rowzz", "col1", "7")); //4
          IntraRes res = null;
          try {
            res = ic.sendBlocking(req);
          } catch (Exception e) {
            e.printStackTrace();
          }
        }
      }
    };
    t.start();
    t.join();
    long end = System.currentTimeMillis();
    System.out.println(end-start);
  }
 
  
	 @Test
	 @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	   public void optioanKSandCSTest() {
	     IntraReq req = new IntraReq();
	    
	     req.add( Operations.setAutotimestampOp() ); //0
	     req.add( Operations.assumeOp("myks", "mycf", "value", "UTF-8"));//1
	     req.add( Operations.assumeOp("myks", "mycf", "column", "int32"));//2
	     IntraOp setOp = Operations.setOp("optional", 1, "wow"); //3
	     setOp.set("keyspace", "myks");
	     setOp.set("columnfamily", "mycf");
	     req.add( setOp );
	     //opa sexyy builder style
	     req.add( Operations.getOp("optional", 1)
	    		 .set("keyspace", "myks")
	    		 .set("columnfamily", "mycf")); //4
	     IntraRes res = new IntraRes();
	     is.handleIntraReq(req, res, x);
	     List<Map> x = (List<Map>) res.getOpsRes().get(4);
	     
	     Assert.assertEquals( "wow",  x.get(0).get("value") );
	     Assert.assertEquals( 1,  x.get(0).get("name") );
	   }
	 
	 
	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void componentTest() {
		IntraReq req = new IntraReq();
		req.add(Operations.setAutotimestampOp()); // 0
		req.add(Operations.assumeOp("myks", "mycf", "value", "UTF-8"));// 1
		req.add(Operations.assumeOp("myks", "mycf", "column", "int32"));// 2
		IntraOp setOp = Operations.setOp("optional", 1, "wow"); // 3
		setOp.set("keyspace", "myks");
		setOp.set("columnfamily", "mycf");
		req.add(setOp);
		Set<String> wanted = new HashSet<String>();
		wanted.addAll(Arrays.asList(new String[]{"value", "timestamp"}));
		req.add( Operations.componentSelect(wanted)); //4
		// opa sexyy builder style
		req.add(Operations.getOp("optional", 1).set("keyspace", "myks")
				.set("columnfamily", "mycf")); // 5
		IntraRes res = new IntraRes();
		is.handleIntraReq(req, res, x);
		List<Map> x = (List<Map>) res.getOpsRes().get(5);

		Assert.assertEquals("wow", x.get(0).get("value"));
		Assert.assertEquals(true, x.get(0).containsKey("timestamp"));
		Assert.assertTrue( (Long)x.get(0).get("timestamp") > 0);
	}
	
	@Test
	@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
	public void cql3Schema() {
		IntraReq req = new IntraReq();
		String ks = "cqltesting";
		req.add( Operations.setKeyspaceOp("system"));//0
		req.add( Operations.createKsOp(ks, 1) );//1
		req.add( Operations.setKeyspaceOp(ks) ); //2
		String videos= "CREATE TABLE videos ( "+
				  " videoid varchar, "+
				  " videoname varchar, "+
				  " username varchar, "+
				  " description int, "+
				  " tags varchar, "+
				  " PRIMARY KEY (videoid,videoname) "+ 
				" ); ";
		req.add(Operations.cqlQuery(videos, "3.0.0"));//3
		//String query = "SELECT columnfamily_name, comparator, default_validator, key_validator FROM system.schema_columnfamilies WHERE keyspace_name='%s'";
		//String formatted = String.format(query, ks);
		//req.add( Operations.setKeyspaceOp("system"));//4
		//req.add( Operations.cqlQuery(formatted, "3.0.0").set("convert", "")); //5
		String videoIns = "INSERT INTO videos (videoid,videoname,tags) VALUES (1,'weekend','fun games')"; 
		String videoIns1 = "INSERT INTO videos (videoid,videoname,tags) VALUES (2,'weekend2','fun games returns')";
		req.add( Operations.cqlQuery(videoIns, "3.0.0") );
		req.add( Operations.cqlQuery(videoIns1, "3.0.0") );
		req.add( Operations.cqlQuery("select * from videos WHERE videoid=2", "3.0.0").set("convert", true) );
		IntraRes res = new IntraRes();
		is.handleIntraReq(req, res, x);
		System.out.println(res.getException());
		List<Map> results = (List<Map>) res.getOpsRes().get(6) ;
		Assert.assertEquals("videoid", results.get(0).get("name"));
		Assert.assertEquals("2", results.get(0).get("value"));	
		
	}
	
	
	@Test
	public void timeoutOpTest() throws CharacterCodingException {
		IntraReq req = new IntraReq();
		req.add(Operations.setKeyspaceOp("timeoutks")); // 0
		req.add(Operations.createKsOp("timeoutks", 1)); // 1
		req.add(Operations.createCfOp("timeoutcf")); // 2
		req.add(Operations.setColumnFamilyOp("timeoutcf")); // 3
		req.add(Operations.setAutotimestampOp()); // 4
		req.add(Operations.assumeOp("timeoutks", "timeoutcf", "value", "UTF-8"));// 5
		req.add(Operations.setOp("rowa", "col1", "20")); // 6
		req.add(Operations.setOp("rowa", "col2", "22")); // 7
		req.add(Operations
				.createFilterOp(
						"ALongOne",
						"groovy",
						"public class ALongOne implements org.usergrid.vx.experimental.Filter { \n"
								+ " public Map filter(Map row){ \n"
								+ "   Thread.sleep(5000); \n"
								+ "   return null; \n"
								+ "} }\n")); // 8
		req.add(Operations.filterModeOp("ALongOne", true)); // 9
		req.add(Operations.sliceOp("rowa", "col1", "col3", 10).set("timeout", 3000)); // 10
		IntraRes res = new IntraRes();
		is.handleIntraReq(req, res, x);
		Assert.assertNotNull(res.getException());
		Assert.assertEquals(new Integer(10), res.getExceptionId());
	}
}