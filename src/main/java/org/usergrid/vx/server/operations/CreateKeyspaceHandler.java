package org.usergrid.vx.server.operations;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.cassandra.config.CFMetaData;
import org.apache.cassandra.config.KSMetaData;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.service.MigrationManager;
import org.apache.cassandra.thrift.KsDef;
import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;

public class CreateKeyspaceHandler implements Handler<Message<JsonObject>> {

    @Override
    public void handle(Message<JsonObject> event) {
        JsonObject params = event.body.getObject("op");
        Integer id = event.body.getInteger("id");
        JsonObject state = event.body.getObject("state");
        String keyspace = params.getString("name");
        int replication = params.getInteger("replication");

        JsonObject response = new JsonObject();

        Collection<CFMetaData> cfDefs = new ArrayList<CFMetaData>(0);
        KsDef def = new KsDef();
        def.setName(keyspace);
        def.setStrategy_class("SimpleStrategy");
        Map<String, String> strat = new HashMap<String, String>();
        //TODO we should be able to get all this information from the client
        strat.put("replication_factor", Integer.toString(replication));
        def.setStrategy_options(strat);
        KSMetaData ksm = null;
        try {
            ksm = KSMetaData.fromThrift(def,
                cfDefs.toArray(new CFMetaData[cfDefs.size()]));
        } catch (ConfigurationException e) {
            response.putString("exception", e.getMessage());
            response.putNumber("exceptionId", id);
            event.reply(response);
            return;
        }

        try {
            MigrationManager.announceNewKeyspace(ksm);
        } catch ( ConfigurationException e ) {
            response.putString("exception", e.getMessage());
            response.putNumber("exceptionId", id);
            event.reply(response);
            return;
        }

        response.putString(id.toString(), "OK");
        event.reply(response);
    }

}
