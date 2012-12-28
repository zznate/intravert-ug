package org.usergrid.vx.experimental;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.service.MigrationManager;
import org.junit.Ignore;
import org.junit.runner.Description;
import org.junit.runner.notification.RunNotifier;
import org.junit.runners.BlockJUnit4ClassRunner;
import org.junit.runners.model.FrameworkMethod;
import org.junit.runners.model.InitializationError;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.server.IntravertDeamon;

import java.io.File;
import java.lang.annotation.Annotation;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * @author zznate
 */
public class CassandraRunner extends BlockJUnit4ClassRunner {

  private static final Logger logger = LoggerFactory.getLogger(CassandraRunner.class);

  static IntraService is = new IntraService();
  static IntravertDeamon intravertDeamon;
  static ExecutorService executor = Executors.newSingleThreadExecutor();

  public CassandraRunner(Class<?> klass) throws InitializationError {
    super(klass);
    logger.info("CassandraRunner constructed with class {}", klass.getName());
  }

  @Override
  protected void runChild(FrameworkMethod method, RunNotifier notifier) {
    logger.info("runChild invoked on method: " + method.getName());
    RequiresKeyspace rk = method.getAnnotation(RequiresKeyspace.class);
    RequiresColumnFamily rcf = method.getAnnotation(RequiresColumnFamily.class);
    if ( rk != null ) {
      maybeCreateKeyspace(rk, rcf);
    }

    if (method.getAnnotation(DataLoader.class) != null) {
      logger.info("Loading dataset {}", method.getAnnotation(DataLoader.class).dataset());
    }

    super.runChild(method, notifier);
  }

  @Override
  public void run(RunNotifier notifier) {
    startCassandra();
    RequiresKeyspace rk = null;
    RequiresColumnFamily rcf = null;
    for (Annotation ann : getTestClass().getAnnotations() ) {
      if ( ann instanceof RequiresKeyspace ) {
        rk = (RequiresKeyspace)ann;
      } else if ( ann instanceof RequiresColumnFamily ) {
        rcf = (RequiresColumnFamily)ann;
      }
    }
    if ( rk != null ) {
      maybeCreateKeyspace(rk, rcf);
    }

    super.run(notifier);
  }

  private void maybeCreateKeyspace(RequiresKeyspace rk, RequiresColumnFamily rcf) {
    logger.info("RequiresKeyspace annotation has ksName: {}", rk.ksName());
    List<CFMetaData> cfs = extractColumnFamily(rcf);
    try {
      MigrationManager
              .announceNewKeyspace(KSMetaData.newKeyspace(rk.ksName(),
                      rk.strategy(), KSMetaData.optsWithRF(rk.replication()), false, cfs));
    } catch (AlreadyExistsException aee) {
      logger.error("Will use existing Keyspace for " + rk.ksName());
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create keyspace for " + rk.ksName(),ex);
    }
  }

  private List<CFMetaData> extractColumnFamily(RequiresColumnFamily rcf) {
    logger.info("RequiresColumnFamily annotation has name: {} for ks: {}", rcf.cfName(), rcf.ksName());
    CFMetaData cfm;
    try {
      cfm = new CFMetaData(rcf.ksName(), rcf.cfName(),
            ColumnFamilyType.Standard, TypeParser.parse(rcf.comparator()), null);

    } catch (Exception ex) {
      throw new RuntimeException("Could not create column family for: " + rcf.cfName(), ex);
    }
    return Arrays.asList(cfm);
  }

  private void startCassandra() {
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

  private static boolean deleteRecursive(File path) {
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
