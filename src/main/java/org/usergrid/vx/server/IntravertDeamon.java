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

import org.apache.cassandra.service.CassandraDaemon;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class IntravertDeamon extends CassandraDaemon {

  private static final Logger logger = LoggerFactory.getLogger(IntravertDeamon.class);

  private static final IntravertDeamon instance = new IntravertDeamon();
  public Server intravertServer;
  private static String basePath;

	public static void main(String[] args) {
		System.setProperty("cassandra-foreground", "true");
		System.setProperty("log4j.defaultInitOverride", "true");
		System.setProperty("log4j.configuration", "log4j.properties");
    basePath = System.getProperty("basePath","/intravert");

    instance.activate();
	}

  @Override
  protected void setup() {
    super.setup();
    intravertServer = new IntravertCassandraServer(basePath);
  }

  @Override
  public void init(String[] arguments) throws IOException {
    super.init(arguments);
  }

  @Override
  public void start() {
    intravertServer.start();
  }

  @Override
  public void stop() {
    logger.info("Stopping IntravertDeamon");
    intravertServer.stop();
  }


}