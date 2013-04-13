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
public class AssumeITest {

  @Test
  public void assumeTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("assks")); // 0
    req.add(Operations.createKsOp("assks", 1)); // 1
    req.add(Operations.createCfOp("asscf")); // 2
    req.add(Operations.setColumnFamilyOp("asscf")); // 3
    req.add(Operations.setAutotimestampOp(true)); // 4
    req.add(Operations.assumeOp("assks", "asscf", "value", "UTF8Type"));// 5
    req.add(Operations.setOp("rowa", "col1", "wow")); // 6
    req.add(Operations.getOp("rowa", "col1")); // 7
    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(req);
    @SuppressWarnings({ "unchecked", "rawtypes" })
    List<Map> x = (List<Map>) res.getOpsRes().get(7);
    System.out.println(res);
    Assert.assertEquals("wow", x.get(0).get("value"));
  }

  @Test
  public void assume2CfsTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("system"));
    req.add(Operations.createKsOp("assks1", 1));
    req.add(Operations.createKsOp("assks2", 1));

    req.add(Operations.setKeyspaceOp("assks1"));
    req.add(Operations.createCfOp("asscf1"));

    req.add(Operations.setKeyspaceOp("assks2"));
    req.add(Operations.createCfOp("asscf2"));
    req.add(Operations.setAutotimestampOp(true));
    req.add(Operations.assumeOp("assks1", "asscf1", "value", "UTF8Type"));
    req.add(Operations.assumeOp("assks1", "asscf1", "column", "UTF8Type"));
    req.add(Operations.assumeOp("assks2", "asscf2", "value", "Int32Type"));
    req.add(Operations.assumeOp("assks2", "asscf2", "column", "Int32Type"));

    req.add(Operations.setKeyspaceOp("assks1"));
    req.add(Operations.setColumnFamilyOp("asscf1"));
    req.add(Operations.setOp("rowa", "col1", "wow"));
    req.add(Operations.getOp("rowa", "col1"));

    req.add(Operations.setKeyspaceOp("assks2"));
    req.add(Operations.setColumnFamilyOp("asscf2"));
    req.add(Operations.setOp("rowa", 4, 3));
    req.add(Operations.getOp("rowa", 4));

    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(req);
    System.out.println(res);

  }

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "columnasscf")
  public void columnAssumeTest() throws Exception{
    IntraReq r = new IntraReq();
    r.add( Operations.assumeColumnOp("myks", "columnasscf", "astring", "UTF8Type"))
     .add( Operations.assumeColumnOp("myks", "columnasscf", "aint", "Int32Type"))
     .add( Operations.setKeyspaceOp("myks"))
     .add( Operations.setColumnFamilyOp("columnasscf"))
     .add( Operations.setOp("arow", "astring", "wow") )
     .add( Operations.setOp("arow", "aint", 5) )
     .add( Operations.sliceOp("arow", "a", "b", 2));
    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(r);
    System.out.println(res);
    List<Map> x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals(5, x.get(0).get("value"));
    Assert.assertEquals("wow", x.get(1).get("value"));
  }
}
