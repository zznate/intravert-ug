package org.usergrid.vx.experimental;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class FilterITest {

  @Test
  public void executeJavaScriptFilter() throws Exception {
      IntraReq req = new IntraReq();
      req.add(Operations.setKeyspaceOp("jsFilterks")); //0
      req.add(Operations.createKsOp("jsFilterks", 1)); //1
      req.add(Operations.createCfOp("filtercf")); //2
      req.add(Operations.setColumnFamilyOp("filtercf")); //3
      req.add(Operations.setAutotimestampOp(true)); //4
      req.add(Operations.assumeOp("jsFilterks", "filtercf", "value", "UTF8Type"));//5
      req.add(Operations.setOp("rowa", "col1", "20")); //6
      req.add(Operations.setOp("rowa", "col2", "22")); //7
      req.add(Operations.createFilterOp("over21", "javascript",
          "function over21(row) { if (row['value'] > 21) return row; else return null; }")); // 8
      req.add(Operations.filterModeOp("over21", true)); //9
      req.add(Operations.sliceOp("rowa", "col1", "col3", 10)); //10
      IntraClient2 ic2 = new IntraClient2("localhost", 8080);
      IntraRes res = ic2.sendBlocking(req);
      System.out.println(res.getException());
      List<Map> results = (List<Map>) res.getOpsRes().get(10);
      Assert.assertEquals("22", results.get(0).get("value"));
      Assert.assertEquals(1, results.size());
  }
  
  @Ignore
  @Test
  /* why wont this work */
  public void executeJavaScriptPhoneFilter() throws Exception {
      IntraReq req = new IntraReq();
      req.add(Operations.setKeyspaceOp("jsFilterks2")); //0
      req.add(Operations.createKsOp("jsFilterks2", 1)); //1
      req.add(Operations.createCfOp("filtercf2")); //2
      req.add(Operations.setColumnFamilyOp("filtercf2")); //3
      req.add(Operations.setAutotimestampOp(true)); //4
      req.add(Operations.assumeOp("jsFilterks2", "filtercf2", "value", "UTF8Type"));//5
      req.add(Operations.setOp("rowa", "col1", "{ \"last\" : \"Peters\", \"first\" : \"john\", \"pnumber\": \"914-555-5555\" }")); //6
      req.add(Operations.setOp("rowa", "col2", "{ \"last\" : \"Peters2\", \"first\" : \"john\", \"pnumber\": \"915-555-5555\" }")); //6
      /*
      req.add(Operations.createFilterOp("over22", "javascript",
          "function over22(row) { x = JSON.parse( row['value'], null ); \n" +
          "if ( String(x.pnumber).search(\"914\") != -1 ) { return row } else { return null }  \n" +
          "}")); // 8
          */
      //req.add(Operations.sliceOp("rowa", "col1", "col3", 10)); //10
      req.add(Operations.createFilterOp("over22", "javascript",
              "function over22(row) { if ( /914/.test(row['value'])  ) {return row;} else {return null;} }  " )); // 8
      req.add(Operations.filterModeOp("over22", true)); //9
      req.add(Operations.sliceOp("rowa", "col1", "col3", 10)); //10
      IntraClient2 ic2 = new IntraClient2("localhost", 8080);
      IntraRes res = ic2.sendBlocking(req);
      System.out.println(res.getException());
      
      List<Map> results = (List<Map>) res.getOpsRes().get(10);
      System.out.println(results);
      Assert.assertEquals("22", results.get(0).get("value"));
      Assert.assertEquals(1, results.size());
  }
  
  
}
