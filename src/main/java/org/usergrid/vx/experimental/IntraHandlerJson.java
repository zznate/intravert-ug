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
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

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
		request.bodyHandler( new Handler<Buffer>() {
			public void handle(Buffer buffer) {
                            boolean asyncRequestsEnabled = Boolean.valueOf(
                                System.getProperty("async-requests-enabled", "false"));

                            if (asyncRequestsEnabled || true) {
                                handleRequestAsync(request, buffer);
                            } else {
                                handleRequest(request, buffer);
                            }
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
                operation.putNumber("id", idGenerator.get());
                operation.putObject("state", new JsonObject()
                    .putArray("components", new JsonArray()
                        .add("name")
                        .add("value")));
                idGenerator.incrementAndGet();

                OperationsRequestHandler operationsRequestHandler = new OperationsRequestHandler(idGenerator,
                    operations, event);
                TimeoutHandler timeoutHandler = new TimeoutHandler(operationsRequestHandler);
                long timerId = vertx.setTimer(10000, timeoutHandler);
                operationsRequestHandler.setTimerId(timerId);

                vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation,
                    operationsRequestHandler);
            }
        });
    }

    private class TimeoutHandler implements Handler<Long> {

        private OperationsRequestHandler operationsRequestHandler;

        public TimeoutHandler(OperationsRequestHandler operationsRequestHandler) {
            this.operationsRequestHandler = operationsRequestHandler;
        }

        @Override
        public void handle(Long timerId) {
            operationsRequestHandler.timeout();
        }
    }

    private class OperationsRequestHandler implements Handler<Message<JsonObject>> {

        private AtomicInteger idGenerator;

        private JsonArray operations;

        private Message<JsonObject> originalMessage;

        private JsonObject results;

        private JsonObject state;

        private boolean timedOut = false;

        private ReentrantLock timeoutLock = new ReentrantLock();

        private long timerId;

        public OperationsRequestHandler(AtomicInteger idGenerator, JsonArray operations,
            Message<JsonObject> originalMessage) {
            this.idGenerator = idGenerator;
            this.operations = operations;
            this.originalMessage = originalMessage;

            results = new JsonObject();
            results.putObject("opsRes", new JsonObject());
            results.putString("exception", null);
            results.putString("exceptionId", null);

            state = new JsonObject();
        }

        public void setTimerId(long timerId) {
            this.timerId = timerId;
        }

        @Override
        public void handle(Message<JsonObject> event) {
            vertx.cancelTimer(timerId);

            Integer currentId = idGenerator.get();
            Integer opId = currentId - 1;

            String exceptionId = event.body.getString("exceptionId");
            String exception = event.body.getString("exception");

            if (exception != null || exceptionId != null) {
                results.putString("exception", exception);
                results.putString("exceptionId", exceptionId);

                sendResults();
            }

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
                results.getObject("opsRes").putString(opId.toString(), (String) opResult);
            } else if (opResult instanceof Number) {
                results.getObject("opsRes").putNumber(opId.toString(), (Number) opResult);
            } else if (opResult instanceof JsonObject) {
                results.getObject("opsRes").putObject(opId.toString(), (JsonObject) opResult);
            } else if (opResult instanceof List) {
                results.getObject("opsRes").putArray(opId.toString(), new JsonArray((List) opResult));
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
                TimeoutHandler timeoutHandler = new TimeoutHandler(this);
                timerId = vertx.setTimer(10000, timeoutHandler);
                vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation, this);
            } else {
                sendResults();
            }
        }

        private void sendResults() {
            try {
                timeoutLock.lock();
                if (!timedOut) {
                    originalMessage.reply(results);
                }
            } finally {
                timeoutLock.unlock();
            }
        }

        public void timeout() {
            try {
                timeoutLock.lock();
                timedOut = true;
                results.putString("exception", "Operation timed out.");
                results.putString("exceptionId", Integer.toString(idGenerator.get() - 1));
                originalMessage.reply(results);
            } finally {
                timeoutLock.unlock();
            }
        }
    }
}
