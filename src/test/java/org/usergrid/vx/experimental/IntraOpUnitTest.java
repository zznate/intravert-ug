package org.usergrid.vx.experimental;

import org.junit.Test;

import static junit.framework.Assert.assertTrue;
import static junit.framework.Assert.fail;

/**
 * @author zznate
 */
public class IntraOpUnitTest {

  @Test
  public void validateOk() {
    IntraOp op = Operations.listColumnFamilyOp("mykeyspace");
    op = Operations.consistencyOp("QUORUM");
    op = Operations.createKsOp("mykeyspace",1);
    op = Operations.getOp("rowkey","colname");
    op = Operations.setOp("rowkey","colname","colvalue");
    op = Operations.sliceOp("rowkey","start","end",10);
    op = Operations.sliceOp("rowkey", null, null, 10);
    op = Operations.createCfOp("MyCf");
    op = Operations.createKsOp("MyKeyspace",3);
    try {
      op = Operations.createKsOp("mykeyspace",0);
      fail(op.getType().toString());
    } catch (IllegalArgumentException iae) {} // nom nom nom

    try {
      op = Operations.consistencyOp("foo");
      fail(op.getType().toString());
    } catch (IllegalArgumentException iae) {} // nom nom nom

  }

  @Test
  public void compositeBBTest(){
    IntraService is = new IntraService();
    is.byteBufferForObject(new Object[] {"yo",0, 2,0});
    
  }
}
