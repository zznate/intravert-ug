/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.experimental;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.db.ColumnFamilyType;
import org.apache.cassandra.db.commitlog.CommitLog;
import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.BytesType;
import org.apache.cassandra.db.marshal.CounterColumnType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.AlreadyExistsException;
import org.apache.cassandra.gms.Gossiper;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.service.StorageProxy;
import org.apache.cassandra.service.StorageService;
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
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/**
 * A custom JUnit runner which uses the Intravert testing annotations
 * for managing schema state.
 * Cassandra is started in the foreground. Both the CQL and Thrift transports
 * are disabled.
 *
 * @author zznate
 */
public class CassandraRunner extends BlockJUnit4ClassRunner {

  private static final Logger logger = LoggerFactory.getLogger(CassandraRunner.class);

  static IntraService is = new IntraService();
  static IntravertDeamon intravertDeamon;
  static ExecutorService executor = Executors.newSingleThreadExecutor();

  public CassandraRunner(Class<?> klass) throws InitializationError {
    super(klass);
    logger.debug("CassandraRunner constructed with class {}", klass.getName());
  }

  @Override
  protected void runChild(FrameworkMethod method, RunNotifier notifier) {
    logger.debug("runChild invoked on method: " + method.getName());
    RequiresKeyspace rk = method.getAnnotation(RequiresKeyspace.class);
    RequiresColumnFamily rcf = method.getAnnotation(RequiresColumnFamily.class);
    if ( rk != null ) {
      maybeCreateKeyspace(rk, rcf);
    } else if ( rcf != null ) {
      maybeCreateColumnFamily(rcf);
    }

    if (method.getAnnotation(DataLoader.class) != null) {
      logger.info("Loading dataset {}", method.getAnnotation(DataLoader.class).dataset());
    }

    super.runChild(method, notifier);
  }

  /**
   * The order of events are as follows:
   * - start IntravertDeamon if not started already
   * - create any keyspaces defined as class level annotations
   * - create any column families defined as class level annotations
   * @param notifier
   */
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
    } else if ( rcf != null ) {
      maybeCreateColumnFamily(rcf);
    }

    super.run(notifier);
  }

  /**
   * Create the keyspace and column family in one shot if they are defined together.
   * The RequiresColumnFamily can be null. If the RequiresColumnFamily is not null and
   * the keyspace exists, the column family defined by such will be truncated if so
   * configured.
   */
  private void maybeCreateKeyspace(RequiresKeyspace rk, RequiresColumnFamily rcf) {
    logger.debug("RequiresKeyspace annotation has ksName: {}", rk.ksName());
    List<CFMetaData> cfs = extractColumnFamily(rcf);
    try {
      MigrationManager
              .announceNewKeyspace(KSMetaData.newKeyspace(rk.ksName(),
                      rk.strategy(), KSMetaData.optsWithRF(rk.replication()), false, cfs));
    } catch (AlreadyExistsException aee) {
      logger.info("Will use existing Keyspace for " + rk.ksName());
      if ( cfs.size() > 0 ) {
        maybeTruncateSafely(rcf);
      }
    } catch (Exception ex) {
      throw new RuntimeException("Failed to create keyspace for " + rk.ksName(),ex);
    }
  }

  private List<CFMetaData> extractColumnFamily(RequiresColumnFamily rcf) {
    logger.debug("RequiresColumnFamily annotation has name: {} for ks: {}", rcf.cfName(), rcf.ksName());
    List<CFMetaData> cfms = new ArrayList();
    if ( rcf != null ) {
      try {
        cfms.add(new CFMetaData(rcf.ksName(), rcf.cfName(),
                ColumnFamilyType.Standard, TypeParser.parse(rcf.comparator()), null));

      } catch (Exception ex) {
        throw new RuntimeException("Could not create column family for: " + rcf.cfName(), ex);
      }
    }
    return cfms;
  }

  private void maybeCreateColumnFamily(RequiresColumnFamily rcf) {
    try {
      CFMetaData cfMetaData;
      if ( rcf.isCounter() ) {
        cfMetaData = new CFMetaData(rcf.ksName(), rcf.cfName(),
                      ColumnFamilyType.Standard, TypeParser.parse(rcf.comparator()), null)
                .replicateOnWrite(false).defaultValidator(CounterColumnType.instance);
      } else {
        cfMetaData = new CFMetaData(rcf.ksName(), rcf.cfName(),
                      ColumnFamilyType.Standard, TypeParser.parse(rcf.comparator()), null);
      }
      MigrationManager.announceNewColumnFamily(cfMetaData);
    } catch(AlreadyExistsException aee) {
      logger.info("ColumnFamily already exists for " + rcf.cfName());
      maybeTruncateSafely(rcf);
    } catch (Exception ex) {
      throw new RuntimeException("Could not create column family for: " + rcf.cfName(), ex);
    }
  }

  private void maybeTruncateSafely(RequiresColumnFamily rcf) {
    if ( rcf != null && rcf.truncateExisting() ) {
      try {
        StorageProxy.truncateBlocking(rcf.ksName(), rcf.cfName());
      } catch (Exception ex) {
        throw new RuntimeException("Could not truncate column family: " + rcf.cfName(),ex);
      }
    }
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
    System.setProperty("cassandra.ring_delay_ms","1000");
    System.setProperty("cassandra.start_rpc","true");
    System.setProperty("cassandra.start_native_transport","true");

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
    Runtime.getRuntime().addShutdownHook(new Thread() {
      @Override
      public void run() {
        try {
          logger.error("In shutdownHook");
          stopCassandra();
        } catch (Exception ex) {
          ex.printStackTrace();
        }
      }
    });
  }

  private void stopCassandra() throws Exception {
    if (intravertDeamon != null) {
      intravertDeamon.deactivate();
      StorageService.instance.stopClient();

    }
    executor.shutdown();
    executor.shutdownNow();
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
