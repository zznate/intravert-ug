package org.usergrid.vx.server;

import org.apache.cassandra.service.IMigrationListener;
import org.apache.cassandra.service.MigrationManager;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.vertx.java.core.Vertx;

/**
 * @author zznate
 */
public class IntravertClusterNotifier implements IMigrationListener {
  private static Logger logger = LoggerFactory.getLogger(IntravertClusterNotifier.class);

  private final Vertx vertx;

  private IntravertClusterNotifier(Vertx vertx) {
    this.vertx = vertx;
  }

  public static IntravertClusterNotifier forServer(Vertx vertx) {
    IntravertClusterNotifier icn = new IntravertClusterNotifier(vertx);
    MigrationManager.instance.register(icn);
    logger.info("IntravertClusterNotifier registered with MigrationManager");
    return icn;
  }

  @Override
  public void onCreateKeyspace(String ksName) {
    logger.info("onCreateKeyspace event for: {}", ksName);
  }

  @Override
  public void onCreateColumnFamly(String ksName, String cfName) {
    logger.info("onCreateColumnFamily event for ks: {} and cf: {}", ksName, cfName);
  }

  @Override
  public void onUpdateKeyspace(String ksName) {
    logger.info("onUpdateKeyspace event for ks: {}", ksName);
  }

  @Override
  public void onUpdateColumnFamly(String ksName, String cfName) {
    logger.info("onUpdateColumnFamily for ks: {} and cf: {}", ksName, cfName);
  }

  @Override
  public void onDropKeyspace(String ksName) {
    logger.info("onDropKeyspace for ksName: {}", ksName);
  }

  @Override
  public void onDropColumnFamly(String ksName, String cfName) {
    logger.info("onDropColumnFamily for ksName: {} and cf: {}", ksName, cfName);
  }
}
