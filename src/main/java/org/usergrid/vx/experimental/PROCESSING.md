PROCESSING VERBS
==============

Terminology
----

* CREATEPROCESSOR: Creates and loads a processor
* PROCESS: Instructs a processor to operate on the result of an operation

Example
----
GET and SLICE verbs both return List<Map>. A user wishes to modify the results on the server side before they are
returned to the client. In the case the value of the columns are String and the user wishes to upper-case those values.

API
----
Processor takes a List<Map> as input and produces a List<Map> as output.

    package org.usergrid.vx.experimental;

    import java.util.List;
    import java.util.Map;

    public interface Processor {
      public List<Map> process(List<Map> input);
    }

Statically defined classes would be combersome to manage deploy and undeploy. IntraVert allows Groovy 
code to be defined in Java Strings and sent as part of the request.

     IntraReq req = new IntraReq();
     req.add( IntraOp.setKeyspaceOp("procks") ); //0
     req.add( IntraOp.createKsOp("procks", 1)); //1
     req.add( IntraOp.createCfOp("proccf")); //2
     req.add( IntraOp.setColumnFamilyOp("proccf") ); //3
     req.add( IntraOp.setAutotimestampOp() ); //4
     req.add( IntraOp.assumeOp("procks", "proccf", "value", "UTF-8"));//5
     req.add( IntraOp.setOp("rowa", "col1", "wow")); //6
     req.add( IntraOp.getOp("rowa", "col1")); //7
     
The user uses the createProcessorOp to define a class Capitalize that implements Handler.

     req.add( IntraOp.createProcessorOp("capitalize", "groovy", 
         "public class Capitalize implements org.usergrid.vx.experimental.Processor { \n"+
         "  public List<Map> process(List<Map> input){" +
         "    List<Map> results = new ArrayList<HashMap>();"+
         "    for (Map row: input){" +
         "      Map newRow = new HashMap(); "+
         "      newRow.put(\"value\",row.get(\"value\").toString().toUpperCase());" +
         "      results.add(newRow); "+
         "    } \n" +
         "    return results;"+
         "  }"+
         "}\n"
     ));//8

Next call the processor and instruct referencing the get from operation 7 as input.

     req.add( IntraOp.processOp("capitalize", new HashMap(), 7));//9
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);

The value of the get from step 7 was "wow".

     List<Map> x = (List<Map>) res.getOpsRes().get(7);
     Assert.assertEquals( "wow",  x.get(0).get("value") );
     Assert.assertNull( res.getException() );
     x = (List<Map>) res.getOpsRes().get(9);

The result of step 9 has converted the value to upper case

     Assert.assertEquals( "WOW",  x.get(0).get("value") );
