package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.db.marshal.AbstractCompositeType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.CompositeType;
import org.apache.cassandra.db.marshal.Int32Type;
import org.apache.cassandra.db.marshal.UTF8Type;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;
import org.vertx.java.core.http.impl.ws.Base64;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "compks")
@RequiresColumnFamily(ksName = "compks", cfName = "compcf")
@SuppressWarnings({ "rawtypes", "unchecked" })
public class CompositeITest {

  @Test
  public void compositeTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("compks")); 
    req.add(Operations.setColumnFamilyOp("compcf")); 
    req.add(Operations.setAutotimestampOp(true)); 
    req.add(Operations.assumeOp("compks", "compcf", "value", "CompositeType(UTF8Type,Int32Type)"));
    req.add(Operations.assumeOp("compks", "compcf", "column", "Int32Type"));
    req.add(Operations.setOp("rowa", 1, new Object[] { "yo", 0, 2, 0 })); 
    req.add(Operations.getOp("rowa", 1).set(Operations.USER_OP_ID, "getThat"));

    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(req);
    List<Map> x = (List<Map>) res.getOpsRes().get("getThat");
    Assert.assertEquals(1, x.get(0).get("name"));

    // ByteBuffer bytes = (ByteBuffer) x.get(0).get("value");
    String value = (String) x.get(0).get("value");
    ByteBuffer bytes = ByteBuffer.wrap(Base64.decode(value));

    List<AbstractType<?>> comparators = new ArrayList<>();
    comparators.add(UTF8Type.instance);
    comparators.add(Int32Type.instance);
    CompositeType comparator = CompositeType.getInstance(comparators);

    List<AbstractCompositeType.CompositeComponent> components = comparator.deconstruct(bytes);
    AbstractCompositeType.CompositeComponent c1 = components.get(0);
    AbstractCompositeType.CompositeComponent c2 = components.get(1);

    Assert.assertEquals("yo", c1.comparator.compose(c1.value));
    Assert.assertEquals(2, c2.comparator.compose(c2.value));
  }

  @Test
  public void compositeListTest() throws Exception {
    IntraReq req = new IntraReq();
    req.add(Operations.setKeyspaceOp("compks"));
    req.add(Operations.setColumnFamilyOp("compcf")); 
    req.add(Operations.setAutotimestampOp(true)); 
    req.add(Operations.assumeOp("compks", "compcf", "value", "CompositeType(UTF8Type,Int32Type)"));
    req.add(Operations.assumeOp("compks", "compcf", "column", "Int32Type"));
    req.add(Operations.setOp("rowb", 1, Arrays.asList(new Object[] { "yo", 0, 2, 0 }))); 
    req.add(Operations.getOp("rowb", 1).set(Operations.USER_OP_ID, "getThis"));

    IntraClient2 ic2 = new IntraClient2("localhost", 8080);
    IntraRes res = ic2.sendBlocking(req);
    List<Map> x = (List<Map>) res.getOpsRes().get("getThis");
    Assert.assertEquals(1, x.get(0).get("name"));

    // ByteBuffer bytes = (ByteBuffer) x.get(0).get("value");
    String value = (String) x.get(0).get("value");
    ByteBuffer bytes = ByteBuffer.wrap(Base64.decode(value));

    List<AbstractType<?>> comparators = new ArrayList<>();
    comparators.add(UTF8Type.instance);
    comparators.add(Int32Type.instance);
    CompositeType comparator = CompositeType.getInstance(comparators);

    List<AbstractCompositeType.CompositeComponent> components = comparator.deconstruct(bytes);
    AbstractCompositeType.CompositeComponent c1 = components.get(0);
    AbstractCompositeType.CompositeComponent c2 = components.get(1);

    Assert.assertEquals("yo", c1.comparator.compose(c1.value));
    Assert.assertEquals(2, c2.comparator.compose(c2.value));
  }

  
  
}
