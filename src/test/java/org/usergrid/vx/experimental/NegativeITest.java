package org.usergrid.vx.experimental;

import junit.framework.Assert;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class NegativeITest {

  @Test
  public void testBadSlice() throws Exception{
    IntraReq req = new IntraReq();
    req.add(Operations.setAutotimestampOp(true));
    req.add(Operations.sliceOp("a", "b", "c", 10));
    IntraClient2 ic2 = new IntraClient2("localhost",8080);
    IntraRes ir = ic2.sendBlocking(req);
    Assert.assertEquals("Exception 1", ir.getException());
  }
  
}
