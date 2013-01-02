PROCESSING VERBS
==============

Terminology
----

* CREATEPROCESSOR: Creates and loads a processor
* PROCESS: Instructs a processor to operate on the result of an operation
* CREATEFILTER: Creates and loads a filter
* FILTERMODE: Enables or disables filters 
* CREATEMULTIPROCESSOR: Creates and loads a multi-processor
* MULTIPROCESS: run a multiprocessor

Processor example
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

Filter example
----
Filtering using a Filter operates on rows as they are being read. This has an advantage over processors because it saves memory request and keeps the request size low. For example, imagine a user wants to take the results of a slice and limit the rows in the slice. A filter can do this as the rows are being read.

API
--------
Filter takes as input a Map that represents the column and produces a map or null. Returning null eliminates the column from the list.

    package org.usergrid.vx.experimental;

    import java.util.Map;

    public interface Filter {
      public Map filter(Map row);
    }


    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("filterks") ); //0
    eq.add( IntraOp.createKsOp("filterks", 1)); //1
    req.add( IntraOp.createCfOp("filtercf")); //2
    req.add( IntraOp.setColumnFamilyOp("filtercf") ); //3
    req.add( IntraOp.setAutotimestampOp() ); //4
    req.add( IntraOp.assumeOp("filterks", "filtercf", "value", "UTF-8"));//5

Insert two columns, one with a value of 20 and one with a value greater then 20

    req.add( IntraOp.setOp("rowa", "col1", "20")); //6
    req.add( IntraOp.setOp("rowa", "col2", "22")); //7
    
We will create a filter that returns rows that only have a column value greater then 21.

    req.add( IntraOp.createFilterOp("over21", "groovy", 
     		"public class Over21 implements org.usergrid.vx.experimental.Filter { \n"+
        " public Map filter(Map row){ \n" +
        "   if (Integer.parseInt( row.get(\"value\") ) >21){ \n"+
        "     return row; \n" +
        "   } else { return null; } \n" +
        " } \n" +
        "} \n"
     )); //8

Enabled the filter. The results from any get or slice requests are automatically filtered.

     req.add( IntraOp.filterModeOp("over21", true)); //9
     req.add( IntraOp.sliceOp("rowa", "col1", "col3", 10)); //10
     IntraRes res = new IntraRes();
     is.handleIntraReq(req, res, x);

Notice although the slice should have returned two columns the filter removed one of them.

     List<Map> results = (List<Map>) res.getOpsRes().get(10);
     Assert.assertEquals( "22", results.get(0).get("value") );
     Assert.assertEquals(1, results.size());

MultiProcessor example
----
Processors can operate on the result of one operation. When a user may wishes to combine
the results of two or more operations a MultiProcessor is used.

API
----

A MutliProcess has all the results of all the executed steps (res.getOptRes()). Params
are supplied from the user in the request.

    package org.usergrid.vx.experimental;

    import java.util.List;
    import java.util.Map;

    public interface MultiProcessor {
      public List<Map> multiProcess(Map<Integer,Object> results, Map params);
    }
    
Example
----

The user wishes to run two slice operations and do a server side union of the results. 

    @Test
    @RequiresColumnFamily(ksName = "myks", cfName = "mycf")
    public void multiProcessTest() throws Exception {
      IntraReq req = new IntraReq();
      req.add( IntraOp.setKeyspaceOp("myks") ); //0
      req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
      req.add( IntraOp.setAutotimestampOp() ); //2
      req.add( IntraOp.assumeOp("myks", "mycf", "value", "UTF-8")); //3
      req.add( IntraOp.setOp("rowzz", "col1", "7")); //4
      req.add( IntraOp.setOp("rowzz", "col2", "8")); //5
      req.add( IntraOp.setOp("rowyy", "col4", "9")); //6
      req.add( IntraOp.setOp("rowyy", "col2", "7")); //7
      
 Perform two slice operations.
      
      req.add( IntraOp.sliceOp("rowzz", "a", "z", 100));//8
      req.add( IntraOp.sliceOp("rowyy", "a", "z", 100));//9
 
 Create the union MultiProcess.
    
       req.add( IntraOp.createMultiProcess("union", "groovy", 
       "public class Union implements org.usergrid.vx.experimental.MultiProcessor { \n"+
       "  public List<Map> multiProcess(Map<Integer,Object> results, Map params){ \n"+
       "    java.util.HashMap s = new java.util.HashMap(); \n"+
       "    List<Integer> ids = (List<Integer>) params.get(\"steps\");\n"+
       "    for (Integer id: ids) { \n"+
       "      List<Map> rows = results.get(id); \n"+
       "      for (Map row: rows){ \n"+
       "        s.put(row.get(\"value\"),\"\"); \n"+
       "      } \n"+
       "    } \n"+ 
       "    List<HashMap> ret = new ArrayList<HashMap>(); \n"+
       "    ret.add(s) \n"+
       "    return ret; \n" +
       "  } \n"+
       "} \n" )); //10 

Create the parameters that will tell the MultiProcess what result ids to include.

    Map paramsMap = new HashMap();
    List<Integer> steps = new ArrayList<Integer>();
    steps.add(8);
    steps.add(9);
    paramsMap.put("steps", steps);
    req.add( IntraOp.multiProcess("union", paramsMap)); //11
    
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);

The results contain the results after being send to the processor.

    List<Map> x = (List<Map>) res.getOpsRes().get(11);
    Set<String> expectedResults = new HashSet<String>();
    expectedResults.addAll( Arrays.asList(new String[] { "7", "8", "9"}));
    Assert.assertEquals(expectedResults, x.get(0).keySet());
    
  }
