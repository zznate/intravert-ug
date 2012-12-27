# Intravert
An experimental client and transport for [Apache Cassandra](http://cassandra.apache.org) based on the [vert.x](http://vertx.io) framework. This README is still in progress.

## Overview
Intravert is built on top of Cassandra. It **is not** a fork. Rather, it is a plugin which augments (or replaces - up to you) the default CQL and Thrift transports. 

## Motivations
Intravert was conceived of and designed by long time users of Cassandra who have written numerous real-world applications built on the existing Thrift API. 

This API had it's worts for sure, but it was felt among us that the direction of the Cassandra project with regards to the introduction of CQL sidestepped some of the core reasons we chose Cassandra in the first place. 

Therefore, Intravert is a new approach designed to leverage what we've learned down in the trenches writing (largely Java-based) applications which use Cassandra. Intravert is not intended as a fork, thumb in the eye, or any other detremental action to the Cassandra community or commercial vendors around such. It is simply a purpose built tool to facilitate GetingShitDone™ with real world applications.

Spend some time looking through the documentation, test cases, and examples to see if this approach makes sense for your architecture. 

## Getting Started
You can find a brief overview located here:
<https://github.com/zznate/intravert-ug/tree/master/src/main/java/org/usergrid/vx/experimental/README.md>