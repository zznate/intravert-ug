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
        StringBuilder key = new StringBuilder();
        key.append( HandlerUtils.determineKs(params, state, null));
        key.append( ' ' );
        key.append( HandlerUtils.determineCf(params, state, null));
        key.append( ' ' );
        key.append( params.getString("type") );
        meta.putObject(key.toString(), new JsonObject()
            .putString("clazz", params.getString("clazz")));
        state.putObject("meta", meta);
        event.reply(new JsonObject()
            .putString(id.toString(), "OK")
            .putObject("state", state)
        );
    }
}
