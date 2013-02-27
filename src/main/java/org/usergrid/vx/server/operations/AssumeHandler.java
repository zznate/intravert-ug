package org.usergrid.vx.server.operations;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class AssumeHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        Integer id = event.body.getInteger("id");
        JsonObject params = event.body.getObject("op");

        JsonObject state = event.body.getObject("state");
        JsonObject meta = state.getObject("meta");
        if (meta == null) {
            meta = new JsonObject();
        }
        meta.putObject(params.getString("type"), new JsonObject()
            .putString("keyspace", HandlerUtils.determineKs(params, state))
            .putString("columnfamily", HandlerUtils.determineCf(params, state))
            .putString("type", params.getString("type"))
            .putString("clazz", params.getString("clazz")));
        state.putObject("meta", meta);

        event.reply(new JsonObject()
            .putString(id.toString(), "OK")
            .putObject("state", state)
        );
    }
}
