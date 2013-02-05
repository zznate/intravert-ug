package org.usergrid.vx.client.pooling;

import java.nio.ByteBuffer;
import java.util.*;

/**
 * Derived from the similarly named classes on very well done CQL Java Driver
 * by the good folks at DataStax: https://github.com/datastax/java-driver
 *
 * @author zznate
 */
public class TokenMap {

  private final TokenType tokenType;
  private final Map<Token, Set<String>> tokenToHosts;
  private final List<Comparable<Token>> ring;

  private TokenMap(TokenType tokenType,
                   Map<Token,Set<String>> tokenToHosts,
                   List<Comparable<Token>> allSorted) {
    this.tokenType = tokenType;
    this.ring = allSorted;
    this.tokenToHosts = tokenToHosts;
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
    Map<Token, Set<String>> tokenToHosts = new HashMap<Token, Set<String>>();
    Set<Token> allSorted = new TreeSet<Token>();

    for (Map.Entry<String, Collection<String>> entry : allTokens.entrySet()) {
      String host = entry.getKey();
      for (String tokenStr : entry.getValue()) {
        try {
          Token t = tokenType.fromString(tokenStr);
          allSorted.add(t);
          Set<String> hosts = tokenToHosts.get(t);
          if (hosts == null) {
            hosts = new HashSet<String>();
            tokenToHosts.put(t, hosts);
          }
          hosts.add(host);
        } catch (IllegalArgumentException e) {
          // If we failed parsing that token, skip it
        }
      }
    }
    // Make all the inet set immutable so we can share them publicly safely
    for (Map.Entry<Token, Set<String>> entry: tokenToHosts.entrySet()) {
      entry.setValue(Collections.unmodifiableSet(entry.getValue()));
    }
    return new TokenMap(tokenType, tokenToHosts, new ArrayList(allSorted));

  }

  // TODO update string to Host
  public Set<String> getReplicas(ByteBuffer partitionKey) {
    // Find the primary replica
    int i = Collections.binarySearch(ring, tokenType.hash(partitionKey));
    if (i < 0) {
      i = (i + 1) * (-1);
      if (i >= ring.size())
        i = 0;
    }

    return tokenToHosts.get(ring.get(i));
  }

  // should rebuild for all changes to avoid concurrency issues

}
