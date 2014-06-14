# Intravert
An http based transport and extension language for [Apache Cassandra](http://cassandra.apache.org). 

Note: Some documentation is out of sorts as we rebuild intravert. Refer to unit tests as source of truth.

## Overview
Intravert is built on top of Cassandra. It **is not** a fork rather it augments cassandra by providing simple access to Cassandra via http. It also provides capability to do computation server side in a way similar to co-processors, scanners, and triggers, all features which Cassanra lacks.

## Game Changer Features
Intravert is more then an RPC library, query language, or transport for Cassandra. IntraVert gives access to new powerful ways of working with Cassandra.

* Multiple variations on server side processing allowing users to perform arbitary programatic transformations on the server side before the results are return to the client.

* [GETREF](https://github.com/zznate/intravert-ug/wiki/GETREF) feature can be used to execute procedure and join like logic in a single RPC request.

* A simple sync/async [API and Transport](https://github.com/zznate/intravert-ug/wiki/JSON) that runs over HTTP allows clients to chose from JSON, JSON compressed by smile, and even big fat sexy XML. 

* No more dealing with byte[] or ByteBuffers. Intravert lets users work with simple familiar objects like String or Integer. See [Types and Composites](https://github.com/zznate/intravert-ug/wiki/Composites).

## Motivations

From an application standpoint, if you can't do sparse, wide rows, you break compatibility with 90% of Cassandra applications. So that rules out almost everything; if you can't provide the same data model, you're creating fragmentation, not pluggability. Intravert aims to make the Cassandra data model simply accessable.

Spend some time looking through the documentation, test cases, and examples to see if this approach makes sense for your architecture. 

## Getting Started
You can find a brief overview located here:
<https://github.com/zznate/intravert-ug/wiki>

If you are super impatient, download the source, use maven to install:

    mvn install

Then use maven executor to start the IntravertDeamon thusly:

    mvn -e exec:java -Dexec.mainClass="org.usergrid.vx.server.IntravertDeamon"


To ensure that everything is working, invoke the say_hello.sh script in the examples director:

	./examples/say_hello.sh
	
You can then try some of the additional examples for either the rest or json sub directories. 
