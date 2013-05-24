package org.usergrid.vx.experimental;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;

@Ignore
@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "rmks")
@RequiresColumnFamily(ksName = "rmks", cfName = "rmcf")
public class ResultModeITest {

  
  @Test
  @RequiresColumnFamily(ksName = "rmks", cfName = "rmcf")
  public void filterTest() throws Exception {
    
    IntraReq preq = new IntraReq();
    preq.add(Operations.setKeyspaceOp("rmks"));
    preq.add(Operations.createCfOp("resultcf"));
    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    ic2.sendBlocking(preq);
   
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("rmks")); 
    req.add(Operations.setColumnFamilyOp("rmcf")); 
    req.add(Operations.setAutotimestampOp(true)); 
    req.add(Operations.assumeOp("rmks", "rmcf", "value", "UTF8Type"));
    req.add(Operations.setOp("rowa", "col1", "20")); 
    req.add(Operations.setOp("rowa", "col2", "22")); 
    req.add(Operations
        .createFilterOp("over21", "groovy",
                "{ row -> if (row['value'].toInteger() > 21) return row else return null }")); // 8
    req.add(Operations.filterModeOp("over21", true)); 
    req.add(Operations.resultMode("rmks", "resultcf", true));
    req.add(Operations.sliceOp("rowa", "col1", "col3", 10));   
    IntraRes res = ic2.sendBlocking(req);
    
    IntraReq r3 = new IntraReq();
    r3.add(Operations.setKeyspaceOp("rmks"));
    r3.add(Operations.setColumnFamilyOp("resultcf"));
    req.add(Operations.sliceOp("rowa", "col1", "col3", 10).set(Operations.USER_OP_ID, "wombats")); 
    IntraRes res3 = ic2.sendBlocking(r3);
    List<Map> results = (List<Map>) res3.getOpsRes().get("wombats");
    Assert.assertEquals("22", results.get(0).get("value"));
    Assert.assertEquals(1, results.size());   
  }
}
