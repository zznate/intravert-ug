package org.usergrid.vx.handler.http;

import org.vertx.java.core.Handler;

public class TimeoutHandler implements Handler<Long> {

    private OperationsRequestHandler operationsRequestHandler;

    public TimeoutHandler(OperationsRequestHandler operationsRequestHandler) {
        this.operationsRequestHandler = operationsRequestHandler;
    }

    @Override
    public void handle(Long timerId) {
        operationsRequestHandler.timeout();
    }
}
