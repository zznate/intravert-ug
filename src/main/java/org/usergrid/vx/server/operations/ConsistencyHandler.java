package org.usergrid.vx.server.operations;

import org.vertx.java.core.Handler;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.json.JsonObject;
/*
 * The consistency verb changes the consistency level of the state.
 */
public class ConsistencyHandler implements Handler<Message<JsonObject>> {

	@Override
	public void handle(Message<JsonObject> event) {
		Integer id = event.body.getInteger("id");
		JsonObject params = event.body.getObject("op");
		JsonObject state = event.body.getObject("state");
		state.putString("consistency", params.getString("level") );
		event.reply(new JsonObject().putString(id.toString(), "OK").putObject(
				"state", state));
	}

}
