package org.usergrid.vx.experimental;

import org.apache.cassandra.db.ConsistencyLevel;

/* class that holds properties for the request lifecycle */
public class IntraState {

	String currentKeyspace="";
	String currentColumnFamily="";
	boolean autoTimestamp= true;
	long nanotime = System.nanoTime();
	ConsistencyLevel consistency= ConsistencyLevel.ONE;
}
