package org.usergrid.vx.server.operations;

import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
/*
 * The consistency verb changes the consistency level of the state.
 */
public class ConsistencyHandler extends AbstractIntravertHandler {

	@Override
	public void handleUser(Message<JsonObject> event) {
		Integer id = event.body.getInteger("id");
		JsonObject params = event.body.getObject(Operations.OP);
		JsonObject state = event.body.getObject(Operations.STATE);
		HandlerUtils.instance.setConsistencyLevel(state, params.getString("level"));	
		event.reply(new JsonObject().putString(id.toString(), "OK").putObject(
				Operations.STATE, state));
	}

}
