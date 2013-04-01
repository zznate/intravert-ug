package org.usergrid.vx.experimental;

import junit.framework.Assert;

import org.apache.cassandra.thrift.Cassandra;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.thrift.FramedConnWrapper;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class ThriftITest {

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void aThriftSanityTest() throws Exception {
    Thread.sleep(1000);
    FramedConnWrapper wrap = new FramedConnWrapper("localhost",9160);
    wrap.open();
    Cassandra.Client c = wrap.getClient();
    Assert.assertTrue( c.describe_keyspaces().size() > 1 );
    wrap.close();
  }
}
