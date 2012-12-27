package org.usergrid.vx.experimental;

import org.junit.BeforeClass;
import org.usergrid.vx.server.IntravertDeamon;

import java.io.File;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author zznate
 */
public abstract class BaseIntravertIntegrationTest {

  static IntraService is;
  static IntravertDeamon intravertDeamon = new IntravertDeamon();
  static final AtomicBoolean inited = new AtomicBoolean(false);

 	@BeforeClass
 	public static void before(){
     if ( !inited.get() ) {
       deleteRecursive(new File("/tmp/intra_cache"));
       deleteRecursive(new File ("/tmp/intra_data"));
       deleteRecursive(new File ("/tmp/intra_log"));
       System.setProperty("cassandra-foreground", "true");
       System.setProperty("log4j.defaultInitOverride","true");
       System.setProperty("log4j.configuration", "log4j.properties");
       intravertDeamon.activate();
       is = new IntraService();
       inited.set(true);
     }
 	}

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


}
