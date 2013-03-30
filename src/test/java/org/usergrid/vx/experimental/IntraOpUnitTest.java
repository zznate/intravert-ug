/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.experimental;

import org.junit.Test;
import org.usergrid.vx.server.operations.HandlerUtils;

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
    HandlerUtils.byteBufferForObject(new Object[] {"yo",0, 2,0});
  }
}
