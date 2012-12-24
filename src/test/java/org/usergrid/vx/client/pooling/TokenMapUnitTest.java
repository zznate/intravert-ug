package org.usergrid.vx.client.pooling;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * @author zznate
 */
public class TokenMapUnitTest {

  Map<String,Collection<String>> allTokens;

  @Before
  public void setupLocal() {
    allTokens = new HashMap();
    allTokens.put("host1", Arrays.asList("0","113427455640312821154458202477256070485"));
    allTokens.put("host2", Arrays.asList("56713727820156407428984779325531226112","0"));
    allTokens.put("host3", Arrays.asList("113427455640312821154458202477256070485","56713727820156407428984779325531226112"));
  }

  @Test
  public void buildSimpleThreeRFTwoCluster() {
    TokenMap tokenMap = TokenMap.build(TokenType.RP, allTokens);
    assertEquals(2, tokenMap.getReplicas(ByteBufferUtil.bytes("2835686391007827")).size());
  }
}
