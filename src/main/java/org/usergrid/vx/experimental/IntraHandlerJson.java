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

	static IntraService is = new IntraService();
	static ObjectMapper mapper = new ObjectMapper();
	
  private Vertx vertx;
  
  public IntraHandlerJson(Vertx vertx){
    super();
    this.vertx=vertx;
      registerRequestHandler();
  }
	
	@Override
	public void handle(final HttpServerRequest request) {
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
						/* 
						 * time to rip band aid
                            boolean asyncRequestsEnabled = Boolean.valueOf(
                                System.getProperty("async-requests-enabled", "false"));

                            if (asyncRequestsEnabled || true) {
                                handleRequestAsync(request, buffer);
                            } else {
                                handleRequest(request, buffer);
                            }
                            */
			     handleRequestAsync(request, buffer);
			}
		});
	}

    private void handleRequest(HttpServerRequest request, Buffer buffer) {
        IntraRes res = new IntraRes();
        IntraReq req = null;
        try {
            req = mapper.readValue(buffer.getBytes(), IntraReq.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        is.handleIntraReq(req,res,vertx);
        String value = null;
        try {
            value = mapper.writeValueAsString(res);
        } catch (IOException e) {
            e.printStackTrace();
        }
        request.response.end(value);
    }

    private void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
        IntraReq req = null;
        //JsonObject j = new JsonObject(buffer);
        try {
            req = mapper.readValue(buffer.getBytes(), IntraReq.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
            @Override
            public void handle(Message<JsonObject> event) {
                request.response.end(event.body.toString());
            }
        });
    }

  private void registerRequestHandler() {
    vertx.eventBus().registerHandler("request.json", new Handler<Message<JsonObject>>() {
      @Override
      public void handle(Message<JsonObject> event) {
        AtomicInteger idGenerator = new AtomicInteger(0);
        JsonArray operations = event.body.getArray("e");
        JsonObject operation = (JsonObject) operations.get(idGenerator.get());
        Long timeout = getOperationTime(operation);

        operation.putNumber("id", idGenerator.get());
        operation.putObject("state", new JsonObject()
            .putArray("components", new JsonArray()
                .add("name")
                .add("value")));
        idGenerator.incrementAndGet();

        OperationsRequestHandler operationsRequestHandler = new OperationsRequestHandler(idGenerator,
            operations, event, vertx);
        TimeoutHandler timeoutHandler = new TimeoutHandler(operationsRequestHandler);
        long timerId = vertx.setTimer(timeout, timeoutHandler);
        operationsRequestHandler.setTimerId(timerId);

        vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation,
            operationsRequestHandler);
      }
    });
  }

  // This method is currently duplicated in OperationsRequestHandler. We could move it to
  // HandlerUtils but I am holding off for now because if/when we start using strongly
  // typed objects again for the request, response, etc., this method would make sense
  // as a property if a parameters object if a such a class is introduced.
  private Long getOperationTime(JsonObject operation) {
    JsonObject params = operation.getObject("op");
    Long timeout = params.getLong("timeout");
    if (timeout == null) {
      timeout = 10000L;
    }
    return timeout;
  }

}
