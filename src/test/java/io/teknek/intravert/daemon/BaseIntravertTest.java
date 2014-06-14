package io.teknek.intravert.daemon;

import java.io.File;

import org.apache.cassandra.io.util.FileUtils;
import org.junit.BeforeClass;

import io.teknek.intravert.daemon.IntravertDaemon;

public abstract class BaseIntravertTest {
  public static IntravertDaemon intravert;
  
  private static void deleteIfExists(String path){
    if (!path.startsWith("target"))
      return;
    File f = new File(path);
    if (f.exists())
      FileUtils.deleteRecursive(new File(path));
  }
  
  @BeforeClass
  public static void beforeClass(){
    if (intravert == null){
      deleteIfExists("target/intra_log");
      deleteIfExists("target/intra_data");
      deleteIfExists("target/intra_cache");
      //This controls where system.out goes.
      System.setProperty("cassandra-foreground", "true");
      intravert = new IntravertDaemon();
      intravert.activate();
    }
  }
  
}
