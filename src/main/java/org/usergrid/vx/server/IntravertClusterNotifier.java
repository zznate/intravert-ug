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
  public void onUpdateKeyspace(String ksName) {
    logger.info("onUpdateKeyspace event for ks: {}", ksName);
  }

  

  @Override
  public void onDropKeyspace(String ksName) {
    logger.info("onDropKeyspace for ksName: {}", ksName);
  }

  

  @Override
  public void onCreateColumnFamily(String ksName, String cfName) {
     logger.info("onCreateColumnFamily for ksName: {} and cf: {}", ksName, cfName);
    
  }

  @Override
  public void onDropColumnFamily(String ksName, String cfName) {
    logger.info("onDropColumnFamily for ksName: {} and cf: {}", ksName, cfName);
    
  }

  @Override
  public void onUpdateColumnFamily(String ksName, String cfName) {
    logger.info("onUpdateColumnFamily for ks: {} and cf: {}", ksName, cfName);
    
  }
}
