package org.usergrid.vx.client.pooling;

import org.apache.cassandra.utils.ByteBufferUtil;
import org.apache.cassandra.utils.FBUtilities;
import org.apache.cassandra.utils.MurmurHash;

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

  M3P {
    @Override
    public Token fromString(String tokenStr) {
      return new Token.M3PToken(Long.parseLong(tokenStr));
    }

    @Override
    public Token hash(ByteBuffer partitionKey) {
      long v = MurmurHash.hash3_x64_128(partitionKey, partitionKey.position(), partitionKey.remaining(), 0)[0];
      return new Token.M3PToken(v == Long.MIN_VALUE ? Long.MAX_VALUE : v);
    }
  },

  OPP {
    @Override
    public Token fromString(String tokenStr) {
      return new Token.OPPToken(ByteBufferUtil.bytes(tokenStr));
    }

    @Override
    public Token hash(ByteBuffer partitionKey) {
      return new Token.OPPToken(partitionKey);
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

  // TODO make this a fromString("")
  static TokenType getTokenType(String partitionerName) {
    // if/elseif for other partitioners and custom
    if (partitionerName.endsWith("Murmur3Partitioner"))
        return M3P;
    else if (partitionerName.endsWith("RandomPartitioner"))
        return RP;
    else if (partitionerName.endsWith("OrderedPartitioner"))
        return OPP;
    else
        return null;
  }

  public abstract Token fromString(String tokenStr);

  public abstract Token hash(ByteBuffer partitionKey);


}
