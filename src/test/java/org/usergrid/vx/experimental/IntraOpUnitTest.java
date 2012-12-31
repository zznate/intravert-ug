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
    IntraOp op = IntraOp.listColumnFamilyOp("mykeyspace");
    op = IntraOp.consistencyOp("QUORUM");
    op = IntraOp.createKsOp("mykeyspace",1);
    op = IntraOp.getOp("rowkey","colname");
    op = IntraOp.setOp("rowkey","colname","colvalue");
    op = IntraOp.sliceOp("rowkey","start","end",10);
    op = IntraOp.sliceOp("rowkey", null, null, 10);
    op = IntraOp.createCfOp("MyCf");
    op = IntraOp.createKsOp("MyKeyspace",3);
    try {
      op = IntraOp.createKsOp("mykeyspace",0);
      fail(op.getType().toString());
    } catch (IllegalArgumentException iae) {} // nom nom nom

    try {
      op = IntraOp.consistencyOp("foo");
      fail(op.getType().toString());
    } catch (IllegalArgumentException iae) {} // nom nom nom

  }

  @Test
  public void compositeBBTest(){
    IntraService is = new IntraService();
    is.byteBufferForObject(new Object[] {"yo",0, 2,0});
    
  }
}
