GET REFERENCE
==============

Terminology
----

* GETREF: Use the result of a get as input for another operation.

Example
----
GET and SLICE verbs both return List<Map>. A user needs to use the value of the first get as input to a subsequent set. Currently from the thrift and CQL API, this operation would require two network round trips, the first to read the data, and then the second to write it back. Intravert can do this in a single request. It can be thought of as a server side procedure or value substitution.

API
----

    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("myks") ); //0
	req.add( IntraOp.setColumnFamilyOp("mycf") ); //1
	req.add( IntraOp.setAutotimestampOp() ); //2
	
We will create a row key for testing. In a real world scenario this row was already created and we would not know the value is "7". 
	
	req.add( IntraOp.setOp("rowa", "col1", "7")); //3

Get the value of "rowa" and "col1". 	

    req.add( IntraOp.getOp("rowa", "col1")); //4

Create a rowkey "rowb" with a column "col2" and a value of the result of operation 4.

	req.add( IntraOp.setOp("rowb", "col2", IntraOp.getResRefOp(5, "value"))); //5

Read this row back and confirm its value.
 
	req.add( IntraOp.getOp("rowb", "col2"));//6
    x = (List<Map>) res.getOpsRes().get(6);
    Assert.assertEquals( "7", ByteBufferUtil.string((ByteBuffer) x.get(0).get("value"))  );
		
Refs can be used in join like scenarios where the second read request uses the value of the first. The cassandra servers will still do two lookups but the client only issues a single request.