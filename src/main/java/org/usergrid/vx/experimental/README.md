IntraVert Experimental client
==============

Terminology
----

* IntraReq: A request object which consists of multiple IntraOp(s)
* IntraOp: An operation like a set, or a slice each has an id
* IntraRes: A response object which consists of multiple OpRes(s)
* OpRes: The result of an IntraOp

Process Flow
----

User constructs and IntraReq with one or more IntraOp(s).
User sends IntraReq to server.
Server processes the request. 
If executing an IntraOp causes an error the processing of the entire request is stopped.
Otherwise the result of each operation is added to the Response.
When all steps are complete the response is returned to the user.

Transports
----

IntraVert is not tightly coupled with a wire-format. The IntraRequest can be serialized 
in either XML or JSON and sent to a specific end point. A binary json (smile) transport
is on the way.

Payload
----

Intravert's payload was designed to be simple. This makes it easy for programs
and even users at a keyboard to interact with the server.

Creating keyspaces and column families
----

Using the JSON wire format we see how easy it is to create keyspaces and column families.

	{"e":[
	  {"id":1,"type":"createkeyspace","op":{"name":"myks","replication":1}},
	  {"id":2,"type":"createcolumnfamily","op":{"name":"mycf"}},
	]}

Java Client
----

The first client is the java client.

	IntraClient i = new IntraClient();
	i.payload="json";
	IntraReq req = new IntraReq();
	req.add( IntraOp.createKsOp("myks", 1));
	req.add( IntraOp.createCfOp("mycf"));
	i.sendBlocking(req) ;
  
`IntraOp` has several static methods which help you build operations quickly.

Object Format
----

IntraVert objects should be "easy" to represent in JSON notation. As a result 
the client code uses many generic objects like List(s) and Map(s) whenever possible.

For example the slice operation in java:

	`req.add( IntraOp.sliceOp("5", "1", "9", 4));`

Looks like this in JSON:

	`{"id":6,"type":"slice","op":{"end":"9","rowkey":"5","size":4,"start":"1"}}`
 
