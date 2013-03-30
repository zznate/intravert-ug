package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.usergrid.vx.client.IntraClient2;

@RunWith(CassandraRunner.class)
@RequiresKeyspace(ksName = "myks")
@RequiresColumnFamily(ksName = "myks", cfName = "mycf")
public class ProcessorITest {

  @Test
  @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
  public void jsonTest() throws Exception {
    String array = "[{\"value\": 1},{\"value\": 2}, {\"value\": 3},{\"value\": 4}]";
    IntraReq req = new IntraReq();
    req.add( Operations.setKeyspaceOp("myks") ); //0
    req.add( Operations.setColumnFamilyOp("mycf") ); //1
    req.add( Operations.setAutotimestampOp(true) ); //2
    req.add( Operations.assumeOp("myks", "mycf", "value", "UTF8Type")); //3
    req.add( Operations.assumeOp("myks", "mycf", "column", "UTF8Type")); //4
    Map row1 = new HashMap();
    row1.put("rowkey", "jsonkey");
    row1.put("name", "data");
    row1.put("value", array);
    
    List<Map> rows = new ArrayList<Map>();
    rows.add(row1);

    req.add( Operations.batchSetOp(rows));//5
    req.add( Operations.sliceOp("jsonkey", "a", "z", 100));//6
    req.add( Operations.createProcessorOp("JsonPathEx", "groovyclassloader", 
            "import com.jayway.jsonpath.*; \n" +
            "public class JsonPathEx implements org.usergrid.vx.experimental.processor.Processor { \n"+
            "  public List<Map> process(List<Map> input){" +
            "    List<Map> results = new ArrayList<HashMap>();"+
            "    for (Map row: input){" +
            "      Map newRow = new HashMap(); "+
            // grovvy requires you to escape $
            "      Integer match = JsonPath.read(row.get(\"value\").toString(), \"\\$.[1].value\"); \n"+
            "      newRow.put(\"value\",match.toString()); \n "+
            "      results.add(newRow); \n"+
            "    } \n" +
            "    return results;"+
            "  }"+
            "}\n"
        ));//7
    req.add( Operations.processOp("JsonPathEx", Collections.EMPTY_MAP, 6));//8

    IntraClient2 ic2 = new IntraClient2("localhost",8080);
    IntraRes res = ic2.sendBlocking(req);
    System.out.println(res);
    List<Map> x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals(1, x.size());
    Assert.assertEquals("data", x.get(0).get("name"));
    Assert.assertEquals(array, x.get(0).get("value"));
    
    List<Map> y = (List<Map>) res.getOpsRes().get(8);
    Assert.assertEquals(1, y.size());
    Assert.assertEquals("2", y.get(0).get("value") );
  }
}
