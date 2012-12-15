package org.usergrid.vx.client.pooling;

import org.junit.Test;

import static junit.framework.Assert.assertEquals;

/**
 * @author zznate
 */
public class TokenUnitTest {

  @Test
  public void tokenEnumLookup() {
    Token t = TokenType.RP.fromString("0");
    Token greaterThan = TokenType.RP.fromString("65910644309090703860934805640084041852");
    Token same = TokenType.RP.fromString("0");
    assertEquals(1, greaterThan.compareTo(t));
    assertEquals(-1, t.compareTo(greaterThan));
    assertEquals(same, t);
  }
}
