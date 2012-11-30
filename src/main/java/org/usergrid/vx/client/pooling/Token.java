package org.usergrid.vx.client.pooling;

import org.apache.cassandra.utils.FBUtilities;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author zznate
 */
public abstract class Token implements Comparable {
/*
  public static Token.Factory getFactory(String partitionerName) {
      if (partitionerName.endsWith("Murmur3Partitioner"))
          //TODO return M3PToken.FACTORY;
        return RPToken.FACTORY;
      else if (partitionerName.endsWith("RandomPartitioner"))
          return RPToken.FACTORY;
      else if (partitionerName.endsWith("OrderedPartitioner"))
          //TODO return OPPToken.FACTORY;
        return RPToken.FACTORY;
      else
          return null;
  }

*/


  public static class RPToken extends Token {
    private final BigInteger value;


    RPToken(BigInteger value) {
      this.value = value;
    }

    @Override
    public int compareTo(Object other) {
      if ( other != null && other instanceof RPToken )
       return value.compareTo(((RPToken)other).value);
      throw new IllegalArgumentException("Attempt to compare wrong token type");
      // TODO type Token<?> ?
      //return value.compareTo(other.value);
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null || this.getClass() != obj.getClass())
        return false;

      return value.equals(((RPToken)obj).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }

  }


}
