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
package org.usergrid.vx.handler.http;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.http.HttpServerRequest;

public class NoMatchHandler implements Handler<HttpServerRequest>{
	
	private final Logger logger = LoggerFactory.getLogger(IntravertCassandraServer.class);

  @Override
  public void handle(HttpServerRequest request) {
    logger.error("no matching endpoint for "+ request.uri());
    request.response().end("No Matching Endpoint.");
  }

}
