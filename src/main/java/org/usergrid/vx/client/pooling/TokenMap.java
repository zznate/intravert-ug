package org.usergrid.vx.client.pooling;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * @author zznate
 */
public class TokenMap {

  private final TokenType tokenType;
  private final Map<Token, Set<String>> tokenToHosts = new HashMap<Token,Set<String>>(); // TODO update to hosts

  private TokenMap(TokenType tokenType) {
    this.tokenType = tokenType;
  }


  /**
   * Can only be constructed internally
   * @param tokenType
   * @param allTokens
   * @return
   */
  // TODO update string to host
  static TokenMap build(TokenType tokenType, Map<String, Collection<String>> allTokens) {
    // TODO loop, build tokens and match to such
    TokenMap tokenMap = new TokenMap(tokenType);

    return tokenMap;
  }

  // TODO update string to Host
  public Set<String> getReplicas(ByteBuffer partitionKey) {
    return null;
  }

  // should rebuild for all changes to avoid concurrency issues

}
