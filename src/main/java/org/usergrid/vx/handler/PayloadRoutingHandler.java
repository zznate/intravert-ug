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
public class PayloadRoutingHandler implements Handler<Message<JsonObject>> {

  public static final String IHJSON_HANDLER_TOPIC = "request.json";
  public static final String REQUEST_HANDLER_HEADER = "operations.";
  
  private final Vertx vertx;

  public PayloadRoutingHandler(Vertx vertx) {
    this.vertx = vertx;
  }

  @Override
  public void handle(Message<JsonObject> event) {
    AtomicInteger idGenerator = new AtomicInteger(0);
    JsonArray operations = event.body.getArray(Operations.E);
    JsonObject operation = (JsonObject) operations.get(idGenerator.get());
    operation.putNumber(Operations.ID, idGenerator.get());
    operation.putObject(Operations.STATE, new JsonObject().putArray(Operations.COMPONENTS,
            new JsonArray().add(Operations.NAME).add(Operations.VALUE)));
    idGenerator.incrementAndGet();
    OperationsRequestHandler operationsRequestHandler = new OperationsRequestHandler(idGenerator,
        operations, event, vertx);
    TimeoutHandler timeoutHandler = new TimeoutHandler(operationsRequestHandler);
    long timerId = vertx.setTimer(HandlerUtils.instance.getOperationTimeout(operation), timeoutHandler);
    operationsRequestHandler.setTimerId(timerId);
    vertx.eventBus().send(new StringBuilder(REQUEST_HANDLER_HEADER).append( operation.getString(Operations.TYPE).toLowerCase()).toString(), operation,
        operationsRequestHandler);
  }
}
