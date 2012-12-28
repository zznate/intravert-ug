package org.usergrid.vx.experimental;

import org.apache.cassandra.service.StorageService;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.usergrid.vx.server.IntravertDeamon;

import java.io.File;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zznate
 */
public abstract class BaseIntravertIntegrationTest {

  static IntraService is = new IntraService();
  static IntravertDeamon intravertDeamon;


  static ExecutorService executor = Executors.newSingleThreadExecutor();


  public static boolean deleteRecursive(File path) {
      if (!path.exists())
      	return false;
      boolean ret = true;
      if (path.isDirectory()){
          for (File f : path.listFiles()){
              ret = ret && deleteRecursive(f);
          }
      }
      return ret && path.delete();
  }

  @BeforeClass
  public static void startCassandra() {
    if ( intravertDeamon != null ) {
      return;
    }
    deleteRecursive(new File("/tmp/intra_cache"));
    deleteRecursive(new File ("/tmp/intra_data"));
    deleteRecursive(new File ("/tmp/intra_log"));
    System.setProperty("cassandra-foreground", "true");
    System.setProperty("log4j.defaultInitOverride","true");
    System.setProperty("log4j.configuration", "log4j.properties");

    executor.execute(new Runnable() {
      public void run() {
        intravertDeamon = new IntravertDeamon();
        intravertDeamon.activate();
      }
    });
    try {
      TimeUnit.SECONDS.sleep(3);
    }
    catch (InterruptedException e) {
      throw new AssertionError(e);
    }


  }


  @AfterClass
  public static void stopCassandra() throws Exception {
    if (intravertDeamon != null) {
      intravertDeamon.deactivate();
      StorageService.instance.stopClient();
    }
    executor.shutdown();
    executor.shutdownNow();
  }


}
