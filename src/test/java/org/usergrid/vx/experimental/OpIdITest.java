package org.usergrid.vx.experimental;

import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;


@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class OpIdITest {

  @Test
  public void assumeTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("myks").set("opid", "a")); 
    req.add(Operations.setColumnFamilyOp("mycf").set("opid", "stuff")); 
    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(req);
    String x = (String) res.getOpsRes().get("a");
    Assert.assertEquals("OK", x );
  }
  
}
