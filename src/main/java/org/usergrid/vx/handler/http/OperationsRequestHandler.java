package org.usergrid.vx.handler.http;

import org.usergrid.vx.experimental.Operations;
import org.usergrid.vx.server.operations.HandlerUtils;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

public class OperationsRequestHandler implements Handler<Message<JsonObject>> {

  private AtomicInteger idGenerator;
  private JsonArray operations;
  private Message<JsonObject> originalMessage;
  private JsonObject results;
  private JsonObject state;
  private boolean timedOut = false;
  private ReentrantLock timeoutLock = new ReentrantLock();
  private long timerId;

  //TODO static ?
  private Vertx vertx;

  public OperationsRequestHandler(AtomicInteger idGenerator, JsonArray operations,
                                  Message<JsonObject> originalMessage, Vertx vertx) {
    this.idGenerator = idGenerator;
    this.operations = operations;
    this.originalMessage = originalMessage;
    this.vertx = vertx;
    results = new JsonObject();
    results.putObject("opsRes", new JsonObject());
    results.putString("exception", null);
    results.putString("exceptionId", null);
    state = new JsonObject();
  }

  public void setTimerId(long timerId) {
    this.timerId = timerId;
  }

  
  @SuppressWarnings("unchecked")
  @Override
  public void handle(Message<JsonObject> event) {
    vertx.cancelTimer(timerId);

    Integer currentId = idGenerator.get();
    Integer opId = currentId - 1;
    String exceptionId = event.body.getString(Operations.EXCEPTION_ID);
    String exception = event.body.getString(Operations.EXCEPTION);
    
    if (exception != null || exceptionId != null) {      
      results.putString(Operations.EXCEPTION, exception);
      results.putString(Operations.EXCEPTION_ID, exceptionId);
      sendResults();
      return;
    }

    Map<String, Object> map = event.body.toMap();
    Object opResult = map.get(String.valueOf(opId));
    String userId = ((JsonObject) operations.get(opId)).getObject(Operations.OP).getString(Operations.USER_OP_ID)  ;
    if (userId == null){
      userId = String.valueOf(opId);
    }
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
      results.getObject(Operations.OPS_RES).putString(userId, (String) opResult);
    } else if (opResult instanceof Number) {
      results.getObject(Operations.OPS_RES).putNumber(userId, (Number) opResult);
    } else if (opResult instanceof JsonObject) {
      results.getObject(Operations.OPS_RES).putObject(userId, (JsonObject) opResult);
    } else if (opResult instanceof List) {
      results.getObject(Operations.OPS_RES).putArray(userId, new JsonArray((List<Object>) opResult));
    } else {
      if (opResult != null){
        throw new IllegalArgumentException(opResult.getClass() + " is not a supported result type");
      } else {
        throw new IllegalArgumentException("No result for operation.");
      }
    }

    if (idGenerator.get() < operations.size()) {
      JsonObject operation = (JsonObject) operations.get(idGenerator.get());
      operation.putNumber(Operations.ID, idGenerator.get());
      if (event.body.getObject(Operations.STATE) != null) {
        state.mergeIn(event.body.getObject(Operations.STATE));
      }
      operation.putObject(Operations.STATE, state.copy()); 
      idGenerator.incrementAndGet();
      TimeoutHandler timeoutHandler = new TimeoutHandler(this);
      timerId = vertx.setTimer(HandlerUtils.instance.getOperationTimeout(operation), timeoutHandler);
      
      HandlerUtils.instance.resolveRefs( operation, results.getObject(Operations.OPS_RES) );
      
      if (operation.getString(Operations.TYPE).equalsIgnoreCase("serviceprocess")) {
        JsonObject params = operation.getObject("op");
        JsonObject theParams = params.getObject("params");
        operation.putObject("mpparams", theParams);
        operation.putObject("mpres", results.getObject("opsRes"));
        vertx.eventBus().send("sps." + params.getString("name").toLowerCase(), operation, this);
      } else if (operation.getString(Operations.TYPE).equalsIgnoreCase("multiprocess")){
        JsonObject params = operation.getObject("op");
        JsonObject theParams = params.getObject("params");
        operation.putObject("mpparams", theParams);
        operation.putObject("mpres", results.getObject("opsRes"));        
        vertx.eventBus().send("multiprocessors." + params.getString("name"), operation, this);
      } else if (operation.getString(Operations.TYPE).equalsIgnoreCase("process")){
        JsonObject params = operation.getObject("op");
        Integer input = params.getInteger("input");
        operation.putArray("input", this.results.getObject("opsRes").getArray(input+"") );
        vertx.eventBus().send("processors." + params.getString("processorname"), operation, this);
      } else {
        vertx.eventBus().send("request." + operation.getString("type").toLowerCase(), operation, this);
      }
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