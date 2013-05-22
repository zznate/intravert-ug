package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class ResultModeHandler extends AbstractIntravertHandler {

  /*
   * (non-Javadoc)
   * @see org.usergrid.vx.server.operations.AbstractIntravertHandler#handleUser(org.vertx.java.core.eventbus.Message)
   * req.add(Operations.resultMode("rmks", "resultcf", true));
   */
  @Override
  public void handleUser(Message<JsonObject> event) {
    JsonObject params = event.body.getObject(Operations.OP);
    JsonObject state = event.body.getObject(Operations.STATE);
    boolean on = params.getBoolean(Operations.ON);
    if (on) {
      HandlerUtils.instance.activateResultMode(state, params.getString(Operations.KEYSPACE), params.getString(Operations.COLUMN_FAMILY));
    } else {
      HandlerUtils.instance.deactivateResultMode(state);
    }
  }

}
