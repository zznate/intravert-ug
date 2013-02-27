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

import org.codehaus.jackson.map.ObjectMapper;
import org.usergrid.vx.handler.http.OperationsRequestHandler;
import org.usergrid.vx.handler.http.TimeoutHandler;
import org.usergrid.vx.server.IntravertCassandraServer;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicInteger;

public class IntraHandlerJson implements Handler<HttpServerRequest>{

	static ObjectMapper mapper = new ObjectMapper();
  private Vertx vertx;
  
  public IntraHandlerJson(Vertx vertx) {
    super();
    this.vertx = vertx;
    IntravertCassandraServer.registerRequestHandler(vertx);
  }
	
	@Override
	public void handle(final HttpServerRequest request) {
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
			     handleRequestAsync(request, buffer);
			}
		});
	}

  private void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    IntraReq req = null;
    try {
      req = mapper.readValue(buffer.getBytes(), IntraReq.class);
    } catch (IOException e) {
      //TODO we need to return something here.
      e.printStackTrace();
    }
    vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        request.response.end(event.body.toString());
      }
    });
  }

}
