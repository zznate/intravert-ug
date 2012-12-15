package org.usergrid.vx.server;

import org.apache.cassandra.service.CassandraDaemon;

public class IntravertServer {

	public static void main(String[] args) {
    System.setProperty("cassandra-foreground", "true");
    System.setProperty("log4j.defaultInitOverride","true");
    System.setProperty("log4j.configuration", "log4j.properties");
    CassandraDaemon.initLog4j();
    
    IntravertServer is = new IntravertServer();
    is.startServer();
	}
	
	private void startServer() {
		IntravertDaemon ucd = new IntravertDaemon();
		ucd.activate();
	}

}