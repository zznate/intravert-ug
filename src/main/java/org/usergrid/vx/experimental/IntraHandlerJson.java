/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.experimental;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.codehaus.jackson.map.SerializationConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.handler.RequestJsonHandler;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

/**
 * The handler is the main entry point for processing the Intravert
 * JSON payload.
 * Specifically, this class:
 * <ol>
 *   <li>Extracts the request body into a {@link IntraReq} object</li>
 *   <li>Sends the IntraReq on the eventBus to the topic
 *     {@link RequestJsonHandler#IHJSON_HANDLER_TOPIC} with an instance
 *     of {@link IHResponse}</li>
 *   <li>IHResponse with send the response via the end method of HttpServerRequest</li>
 * </ol>
 *
 * In debug mode, this class will dump the payload received in the form of what was
 * paesed into {@link IntraReq}
 */
public class IntraHandlerJson implements Handler<HttpServerRequest>{
  private static Logger logger = LoggerFactory.getLogger(IntraHandlerJson.class);

  private static ObjectMapper mapper = new ObjectMapper();
  private static ObjectMapper indentObjectMapper = new ObjectMapper();

  static {
      indentObjectMapper.getSerializationConfig().set(SerializationConfig.Feature.INDENT_OUTPUT, true);
  }

  private final Vertx vertx;

  public IntraHandlerJson(Vertx vertx) {
    this.vertx = vertx;
  }
	
	@Override
	public void handle(final HttpServerRequest request) {
		request.bodyHandler(new Handler<Buffer>() {
      public void handle(Buffer buffer) {
        handleRequestAsync(request, buffer);
      }
    });
	}

  private void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    IntraReq req = null;
    try {
      req = mapper.readValue(buffer.getBytes(), IntraReq.class);
      if ( logger.isDebugEnabled()) {
        logger.debug("IntraJsonHandler received payload: \n{}",
                indentObjectMapper.writeValueAsString(req));
      }
      vertx.eventBus().send(RequestJsonHandler.IHJSON_HANDLER_TOPIC,
              req.toJson(),
              new IHResponse(request));
    } catch (Exception e) {
      request.response.statusCode = BAD_REQUEST.getCode();
      request.response.end(ExceptionUtils.getFullStackTrace(e));
    }
  }

  private static class IHResponse implements Handler<Message<JsonObject>> {

    private final HttpServerRequest request;

    IHResponse(HttpServerRequest request) {
      this.request = request;
    }

    @Override
    public void handle(Message<JsonObject> event) {
      if ( logger.isDebugEnabled()) {
        logger.debug("in IntraHanlderJson's on handler topic {} with event {}",
                RequestJsonHandler.IHJSON_HANDLER_TOPIC,
                event.body.toString() );
      }
      request.response.end(event.body.toString());
    }
  }

}
