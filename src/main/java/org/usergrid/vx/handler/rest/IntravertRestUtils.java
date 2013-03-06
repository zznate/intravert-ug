package org.usergrid.vx.handler.rest;

import org.apache.cassandra.db.ConsistencyLevel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.usergrid.vx.experimental.IntraOp;
import org.usergrid.vx.experimental.IntraReq;
import org.usergrid.vx.experimental.Operations;
import org.vertx.java.core.buffer.Buffer;
import org.vertx.java.core.http.HttpServerRequest;

/**
 * Utility class for extracting useful information from the request for the REST API handlers.
 * 
 * @author zznate
 * @author boneill42
 */
public class IntravertRestUtils {
  private static Logger log = LoggerFactory.getLogger(IntravertRestUtils.class);

  /**
   * Defines the Consistency level header: "X-Consistency-Level"
   */
  public static final String CONSISTENCY_LEVEL_HEADER = "X-Consistency-Level";

  /**
   * Returns the consistency level from the header if present. Defaults to
   * {@link ConsistencyLevel#ONE} if:
   * <ul>
   * <li>The consistency level header is not found</li>
   * <li>The consistency level header is found but {@link ConsistencyLevel#valueOf(String)} throws
   * an IllegalArgumentException (i.e. there was a typo)</li>
   * </ul>
   * 
   * See {@link #CONSISTENCY_LEVEL_HEADER} for the header definition
   * 
   * @param request
   * @return The level specified by the header or ONE according to the conditions defined above.
   */
  public static ConsistencyLevel fromHeader(HttpServerRequest request) {
    if (request.headers().containsKey(CONSISTENCY_LEVEL_HEADER)) {
      try {
        return ConsistencyLevel.valueOf(request.headers().get(CONSISTENCY_LEVEL_HEADER));
      } catch (IllegalArgumentException iae) {
        log.warn("Unable to deduce value for '{}' Header. Using default {} ",
                CONSISTENCY_LEVEL_HEADER, ConsistencyLevel.ONE.toString());
      }
    }
    // TODO this should be a configuration option one day
    return ConsistencyLevel.ONE;
  }



  /**
   * Returns the JSONObject for the event bus that represents a set operation.
   */
  public static IntraReq getReadOperation(HttpServerRequest request, Buffer buffer) {
    String ks = request.params().get(IntraHandlerRest.KEYSPACE);
    String cf = request.params().get(IntraHandlerRest.COLUMN_FAMILY);
    String row = request.params().get(IntraHandlerRest.ROWKEY);
    String col = request.params().get(IntraHandlerRest.COLUMN);

    IntraReq req = new IntraReq();
    if ( log.isDebugEnabled()) {
      log.debug("ReadOperation @ ks=[{}], cf=[{}], row=[{}], col=[{}]",
              new Object[]{ks, cf, row, col});
    }

    if (ks == null){
      log.debug("Listing keyspaces.");
      req.add(Operations.listKeyspacesOp());
    } else if (ks != null) {
      log.debug("Listing column families for [{}]",ks);
      req.add(Operations.listColumnFamilyOp(ks));      
    }    
    return req;
  }

}
