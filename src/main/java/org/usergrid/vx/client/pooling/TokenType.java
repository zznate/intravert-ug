package org.usergrid.vx.client.pooling;

import org.apache.cassandra.utils.FBUtilities;

import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.util.Set;

/**
 * A factory Enum containing the different token types.
 * @author zznate
 */
public enum TokenType {

  RP {
    public Token fromString(String tokenStr) {
      return new Token.RPToken(new BigInteger(tokenStr));
    }

    public Token hash(ByteBuffer partitionKey) {
      return new Token.RPToken(FBUtilities.hashToBigInteger(partitionKey));
    }

  },

  /**
   * Not yet implemented
   */
  CUSTOM {

    @Override
    public Token fromString(String tokenStr) {
      // return new Token.CustomToken(tokenStr)
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Token hash(ByteBuffer partitionKey) {
      return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

  };

  static TokenType getTokenType(String partitionerName) {
    // if/elseif for other partitioners and custom
    return RP;
  }

  public abstract Token fromString(String tokenStr);

  public abstract Token hash(ByteBuffer partitionKey);


}
