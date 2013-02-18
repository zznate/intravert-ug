package org.usergrid.vx.server.operations;

import java.util.List;

import org.apache.cassandra.config.Schema;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonArray;
import org.vertx.java.core.json.JsonObject;

public class ListKeyspacesHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject params = event.body.getObject("op");
        Integer id = event.body.getInteger("id");

        JsonArray keyspaces = new JsonArray();
        for (String ks : Schema.instance.getNonSystemTables()) {
            keyspaces.addString(ks);
        }
        JsonObject response = new JsonObject().putArray(id.toString(), new JsonArray((List)Schema.instance.getNonSystemTables()));

        event.reply(response);
    }
}
