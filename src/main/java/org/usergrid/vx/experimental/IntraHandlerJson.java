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

import java.io.IOException;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

import org.codehaus.jackson.JsonParseException;
import org.codehaus.jackson.map.JsonMappingException;
import org.codehaus.jackson.map.ObjectMapper;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

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
		final IntraRes res = new IntraRes();
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
			  
				IntraReq req = null;
				try {
                                    req = mapper.readValue(buffer.getBytes(), IntraReq.class);
                                    vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
                                        @Override
                                        public void handle(Message<JsonObject> event) {
                                            request.response.end(event.body.toString());
                                        }
                                    });
				} catch (JsonParseException e) {
					e.printStackTrace();
				} catch (JsonMappingException e) {
					e.printStackTrace();
				} catch (IOException e) {
					e.printStackTrace();
				}
				
//				is.handleIntraReq(req,res,vertx);
//
//				String value=null;
//				try {
//					value = mapper.writeValueAsString(res);
//				} catch (JsonGenerationException e1) {
//					e1.printStackTrace();
//				} catch (JsonMappingException e1) {
//					e1.printStackTrace();
//				} catch (IOException e1) {
//					e1.printStackTrace();
//				}
				//System.out.println(value);
//				request.response.end(value);
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
                operation.putNumber("id", idGenerator.get());
                operation.putObject("state", new JsonObject());
                idGenerator.incrementAndGet();

                vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation,
                    new OperationsRequestHandler(idGenerator, operations, event));
            }
        });
    }

    private class OperationsRequestHandler implements Handler<Message<JsonObject>> {

        private AtomicInteger idGenerator;

        private JsonArray operations;

        private Message<JsonObject> originalMessage;

        private JsonObject results;

        private JsonObject state;

        public OperationsRequestHandler(AtomicInteger idGenerator, JsonArray operations,
            Message<JsonObject> originalMessage) {
            this.idGenerator = idGenerator;
            this.operations = operations;
            this.originalMessage = originalMessage;

            results = new JsonObject();
            results.putObject("opRes", new JsonObject());
            results.putString("exception", null);
            results.putString("exceptionId", null);

            state = new JsonObject();
        }

        @Override
        public void handle(Message<JsonObject> event) {
            Integer currentId = idGenerator.get();
            Integer opId = currentId - 1;
            Map<String, Object> map = event.body.toMap();
            Object opResult = map.get(opId.toString());

            // Doing the instanceof check here sucks but there are two reasons why it is
            // here at least for now. First, with this refactoring I do not want to change
            // behavior. To the greatest extent possible, I want integration tests to pass
            // as is. Secondly, I do not want to duplicate logic across each operation
            // handler. So far the operation handler does not need to worry about the
            // format of the response that is sent back to the client. That is done here.
            // The operation handler just provides its own specific response that is keyed
            // off of its operation id.
            //
            // John Sanda
            if (opResult instanceof String) {
                results.getObject("opRes").putString(opId.toString(), (String) opResult);
            } else if (opResult instanceof Number) {
                results.getObject("opRes").putNumber(opId.toString(), (Number) opResult);
            } else if (opResult instanceof JsonObject) {
                results.getObject("opRes").putObject(opId.toString(), (JsonObject) opResult);
            } else {
                throw new IllegalArgumentException(opResult.getClass() + " is not a supported result type");
            }

            if (idGenerator.get() < operations.size()) {
                JsonObject operation = (JsonObject) operations.get(idGenerator.get());
                operation.putNumber("id", idGenerator.get());

                if (event.body.getObject("state") != null) {
                    state.mergeIn(event.body.getObject("state"));
                }
                operation.putObject("state", state.copy());
                idGenerator.incrementAndGet();
                vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation, this);
            } else {
                originalMessage.reply(results);
            }
        }
    }
}
