package io.teknek.intravert.daemon;

import java.io.IOException;

import org.apache.cassandra.service.CassandraDaemon;

public class IntravertDaemon extends CassandraDaemon  {

  private static final IntravertDaemon instance = new IntravertDaemon();
  public IntravertCassandraServer intravertServer;
  
  @Override
  public void init(String[] arguments) throws IOException {
    super.init(arguments);
  }

  @Override
  protected void setup() {
    super.setup();
    intravertServer = new IntravertCassandraServer();
  }

  @Override
  public void start() {
    super.start();
    intravertServer.start();
  }

  @Override
  public void stop() {
    super.stop();
    intravertServer.stop();
  }
  
  public static void main (String [] args){
    instance.activate();
  }
}
