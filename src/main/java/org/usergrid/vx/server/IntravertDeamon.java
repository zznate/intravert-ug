package org.usergrid.vx.server;

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IntravertDeamon extends CassandraDaemon {

  private static final Logger logger = LoggerFactory.getLogger(IntravertDeamon.class);

  private static final IntravertDeamon instance = new IntravertDeamon();
  public Server intravertServer;

	public static void main(String[] args) {
		System.setProperty("cassandra-foreground", "true");
		System.setProperty("log4j.defaultInitOverride", "true");
		System.setProperty("log4j.configuration", "log4j.properties");
		//CassandraDaemon.initLog4j();
    instance.activate();
		//IntravertDeamon is = new IntravertDeamon();

	}

  @Override
  protected void setup() {
    super.setup();
    intravertServer = new IntravertCassandraServer();
  }

  @Override
  public void init(String[] arguments) throws IOException {
    super.init(arguments);    //To change body of overridden methods use File | Settings | File Templates.
  }

  @Override
  public void start() {
    //super.start();
    intravertServer.start();
  }

  @Override
  public void stop() {
    logger.info("Stopping IntravertDeamon");
    intravertServer.stop();
  }


}