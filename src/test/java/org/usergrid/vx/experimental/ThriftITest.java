package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import junit.framework.Assert;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.apache.cassandra.locator.SimpleStrategy;
import org.apache.cassandra.thrift.Cassandra;
import org.apache.cassandra.thrift.CfDef;
import org.apache.cassandra.thrift.Column;
import org.apache.cassandra.thrift.ColumnOrSuperColumn;
import org.apache.cassandra.thrift.ColumnParent;
import org.apache.cassandra.thrift.ConsistencyLevel;
import org.apache.cassandra.thrift.KsDef;
import org.apache.cassandra.thrift.SlicePredicate;
import org.apache.cassandra.thrift.SliceRange;
import org.apache.cassandra.utils.ByteBufferUtil;
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
  
  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void aChangingAComparator() throws Exception {
    
    FramedConnWrapper wrap = new FramedConnWrapper("localhost",9160);
    wrap.open();
    Cassandra.Client c = wrap.getClient();
    KsDef k = new KsDef();
    k.setName("composites");
    Map m = new HashMap();
    m.put("replication_factor","1");
    k.strategy_options = m;
    k.setStrategy_class(SimpleStrategy.class.getName());
    k.cf_defs = new ArrayList<CfDef>();
    c.system_add_keyspace(k);
    
    c.set_keyspace("composites");
    CfDef d = new CfDef();
    d.setKeyspace("composites");
    d.setName("atest");
    d.setComparator_type("CompositeType(UTF8Type,UTF8Type)");
    d.setDefault_validation_class("UTF8Type");
    c.system_add_column_family(d);
    Assert.assertTrue( c.describe_keyspaces().size() > 1 );
    //eds friend bob is a good friend
    List<byte[]> parts = new ArrayList<byte[]>();
    parts.add("friend".getBytes() );
    parts.add("bob".getBytes());
    ColumnParent cp = new ColumnParent();
    cp.setColumn_family("atest");
    Column col = new Column();
    col.setName( CompositeTool.makeComposite(parts) );
    col.setTimestamp(System.nanoTime());
    col.setValue("good".getBytes());
    c.insert(ByteBufferUtil.bytes("ed"), cp, col, ConsistencyLevel.ONE);
    
    
    //eds friend ted is a bad friend
    List<byte[]> parts2 = new ArrayList<byte[]>();
    parts2.add("friend".getBytes() );
    parts2.add("ted".getBytes());
   
 
    Column col2 = new Column();
    col2.setName( CompositeTool.makeComposite(parts2) );
    col2.setTimestamp(System.nanoTime());
    col2.setValue("bad".getBytes());
    c.insert(ByteBufferUtil.bytes("ed"), cp, col2, ConsistencyLevel.ONE);
    
    List<byte[]> parts3 = new ArrayList<byte[]>();
    parts3.add("friend".getBytes() );
    int [] sep = new int[] { 0 , 0 };
    
    SlicePredicate sp = new SlicePredicate();
    SliceRange sr = new SliceRange();
    sr.setStart( ByteBuffer.wrap(CompositeTool.makeComposite(parts3, sep) ) );
    sr.setFinish(new byte[0]);
    sp.setSlice_range(sr);
    
    List<ColumnOrSuperColumn> res = c.get_slice(ByteBufferUtil.bytes("ed"), cp, sp, ConsistencyLevel.ONE);
    Assert.assertEquals(2, res.size());
    
    
    //------------
    
    d.setComparator_type("CompositeType(UTF8Type,UTF8Type,UTF8Type)");
    c.system_update_column_family(d);
    
    List<byte[]> parts4 = new ArrayList<byte[]>();
    parts4.add("friend".getBytes() );
    parts4.add("ted".getBytes());
    parts4.add("yokers".getBytes());
   
 
    Column col4 = new Column();
    col4.setName( CompositeTool.makeComposite(parts4) );
    col4.setTimestamp(System.nanoTime());
    col4.setValue("good".getBytes());
    c.insert(ByteBufferUtil.bytes("ed"), cp, col4, ConsistencyLevel.ONE);
    
    res = c.get_slice(ByteBufferUtil.bytes("ed"), cp, sp, ConsistencyLevel.ONE);
    for (int i =0;i< res.size();i++){
      @SuppressWarnings("rawtypes")
      List<AbstractType> t = new ArrayList<AbstractType>();
      t.add(UTF8Type.instance);
      t.add(UTF8Type.instance);
      t.add(UTF8Type.instance);
      CompositeTool.prettyPrintComposite(res.get(i).column.getName(), t);
    }
    Assert.assertEquals(3, res.size());
    wrap.close();
  }
  
}
