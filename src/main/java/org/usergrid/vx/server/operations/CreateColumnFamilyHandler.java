package org.usergrid.vx.server.operations;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.CfDef;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class CreateColumnFamilyHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject params = event.body.getObject("op");
        Integer id = event.body.getInteger("id");
        JsonObject state = event.body.getObject("state");

        JsonObject response = new JsonObject();

        String cf = params.getString("name");
        CfDef def = new CfDef();
        def.setName(cf);
        String ks = state.getString("currentKeyspace");
        def.setKeyspace(state.getString("currentKeyspace"));
        def.unsetId();
        CFMetaData cfm = null;

        try {
          if (ks == null){ 
            throw new org.apache.cassandra.exceptions.InvalidRequestException("No current keyspace.");
          }
            cfm = CFMetaData.fromThrift(def);
            cfm.addDefaultIndexNames();
        } catch (org.apache.cassandra.exceptions.InvalidRequestException e) {
            response.putString("exception", e.getMessage());
            response.putNumber("exceptionId", id);
            event.reply(response);
            return;
        } catch (ConfigurationException e) {
            response.putString("exception", e.getMessage());
            response.putNumber("exceptionId", id);
            event.reply(response);
            return;
        }
        try {
            MigrationManager.announceNewColumnFamily(cfm);
        } catch (ConfigurationException e) {
            response.putString("exception", e.getMessage());
            response.putNumber("exceptionId", id);
            event.reply(response);
            return;
        }
        response.putString(id.toString(), "OK");
        event.reply(response);
    }

}
