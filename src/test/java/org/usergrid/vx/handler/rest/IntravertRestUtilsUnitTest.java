package org.usergrid.vx.handler.rest;

import org.apache.cassandra.db.ConsistencyLevel;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.vertx.java.core.http.HttpServerRequest;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author zznate
 */
public class IntravertRestUtilsUnitTest {

  private HttpServerRequest req = Mockito.mock(HttpServerRequest.class);
  private Map<String,String> headersMap;

  @Before
  public void setupLocal() {
    headersMap = new HashMap<>();
    headersMap.put(IntravertRestUtils.CONSISTENCY_LEVEL_HEADER,"QUORUM");
  }

  @Test
  public void extractConsisntecyLevel() {
    Mockito.when(req.headers()).thenReturn(headersMap);
    ConsistencyLevel cl = IntravertRestUtils.fromHeader(req);
    assertEquals(ConsistencyLevel.QUORUM, cl);
  }

  @Test
    public void extractDefaultOnEmpty() {
      Mockito.when(req.headers()).thenReturn(new HashMap());
      ConsistencyLevel cl = IntravertRestUtils.fromHeader(req);
      assertEquals(ConsistencyLevel.ONE, cl);
    }

  @Test
  public void extractDefaultOnTypo() {
    headersMap.put(IntravertRestUtils.CONSISTENCY_LEVEL_HEADER, "FORTYTWO");
    Mockito.when(req.headers()).thenReturn(headersMap);
    ConsistencyLevel cl = IntravertRestUtils.fromHeader(req);
    assertEquals(ConsistencyLevel.ONE, cl);
  }
}
