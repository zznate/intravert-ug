package org.usergrid.vx.handler;

import org.usergrid.vx.experimental.Operations;
import org.usergrid.vx.handler.http.OperationsRequestHandler;
import org.usergrid.vx.handler.http.TimeoutHandler;
import org.usergrid.vx.server.operations.HandlerUtils;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * @author zznate
 */
public class RequestJsonHandler implements Handler<Message<JsonObject>> {

  public static final String IHJSON_HANDLER_TOPIC = "request.json";

  private final Vertx vertx;

  public RequestJsonHandler(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void handle(Message<JsonObject> event) {
    AtomicInteger idGenerator = new AtomicInteger(0);
    JsonArray operations = event.body.getArray("e");
    JsonObject operation = (JsonObject) operations.get(idGenerator.get());
    Long timeout = HandlerUtils.getOperationTimeout(operation);

    operation.putNumber(Operations.ID, idGenerator.get());
    operation.putObject(Operations.STATE, new JsonObject()
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
}
