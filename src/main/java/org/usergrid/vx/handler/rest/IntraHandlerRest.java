package org.usergrid.vx.handler.rest;

import static org.jboss.netty.handler.codec.http.HttpResponseStatus.BAD_REQUEST;

import java.util.Map;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.codehaus.jackson.map.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.handler.IntraHandlerBase;
import org.vertx.java.core.Handler;
import org.vertx.java.core.Vertx;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.eventbus.Message;
import org.vertx.java.core.http.HttpServerRequest;
import org.vertx.java.core.json.JsonObject;

/**
 * @author zznate
 * @author boneill42
 */
public class IntraHandlerRest extends IntraHandlerBase {
  private final Logger logger = LoggerFactory.getLogger(IntraHandlerRest.class);
  private static ObjectMapper mapper = new ObjectMapper();
  
  public static final String KEYSPACE = "ks";
  public static final String COLUMN_FAMILY = "cf";
  public static final String ROWKEY = "rawKey";
  public static final String COLUMN = "col";

  public IntraHandlerRest(Vertx vertx) {
    super(vertx);
  }

  public void handleRequestAsync(final HttpServerRequest request, Buffer buffer) {
    Map<String,String> reqParams = request.params();
    logger.debug("Rest request [{}] : [{}]", request.method , reqParams);
    
        
    try {
      IntraReq req = null;
      if(request.method.equals("GET")){
          req = IntravertRestUtils.getReadOperation(request, buffer);
      } else {
        throw new org.apache.cassandra.exceptions.InvalidRequestException("No valid REST operation found.");
      }

      vertx.eventBus().send("request.json", req.toJson(), new Handler<Message<JsonObject>>() {
        @Override
        public void handle(Message<JsonObject> event) {
          request.response.end(event.body.toString());
        }
      });
    } catch (Exception e) {
      request.response.statusCode = BAD_REQUEST.getCode();
      request.response.end(ExceptionUtils.getFullStackTrace(e));
    }
  }

  public void registerRequestHandler() {
    vertx.eventBus().registerHandler("data.keyspaceMeta", new KeyspaceMetaHandler());

  }


}
