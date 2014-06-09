package io.teknek.intravert.daemon;

import org.junit.BeforeClass;

import io.teknek.intravert.daemon.IntravertDaemon;

public abstract class BaseIntravertTest {
  public static IntravertDaemon intravert = new IntravertDaemon();
  
  @BeforeClass
  public static void before(){
    System.setProperty("cassandra-foreground", "true");
    intravert.activate();
  }
  
}
