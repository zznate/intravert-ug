Types made easy including composite types
==============

Intravert does not require the end user to deal with byte arrays. 
Instead users work with familiar types like Integers and Strings. 
IntraVert server handles converting Objects to ByteBuffers for 
your. It even makes working with composites easy, composite types
are specified as Object arrays on the client side.


API
----

 
    IntraReq req = new IntraReq();
    req.add( IntraOp.setKeyspaceOp("compks") ); //0
    req.add( IntraOp.createKsOp("compks", 1)); //1
    req.add( IntraOp.createCfOp("compcf")); //2
    req.add( IntraOp.setColumnFamilyOp("compcf") ); //3
    req.add( IntraOp.setAutotimestampOp() ); //4
    
The assume statement is used to instruct intravert as to what types to
expect when reading column families where column schema is not defined.
    
    req.add( IntraOp.assumeOp("compks", "compcf", "value", "CompositeType(UTF-8,int32)"));//5
    req.add( IntraOp.assumeOp("compks", "compcf", "column", "int32"));//6
    
Composite types are specified as Object Arrays. It is just that easy! No
funky byte buffers that are hard to debug and encode!

    req.add( IntraOp.setOp("rowa", 1, new Object[] {"yo",0, 2,0})); //7
    req.add( IntraOp.getOp("rowa", 1)); //8
    IntraRes res = new IntraRes();
    is.handleIntraReq(req, res, x);
    List<Map> x = (List<Map>) res.getOpsRes().get(8);
    
When the results are returned they are converted to Objects on the server. Again end user 
does not have to deal with ByteBuffers.

    Assert.assertEquals( 1,  x.get(0).get("name") );
    Assert.assertEquals( "yo",  ((Object [])x.get(0).get("value"))[0] );
    Assert.assertEquals( 2,  ((Object [])x.get(0).get("value"))[1] );
   }