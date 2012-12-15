package org.usergrid.vx.client.pooling;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * @author zznate
 */
public abstract class Token implements Comparable {


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

  public static class M3PToken extends Token {
    private final long value;


    M3PToken(long value) {
      this.value = value;
    }

    @Override
    public int compareTo(Object other) {
      if ( other != null && other instanceof M3PToken ) {
        long otherValue = ((M3PToken)other).value;
        return value < otherValue ? -1 : (value == otherValue) ? 0 : 1;
      }
      throw new IllegalArgumentException("Attempt to compare wrong token type");
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null || this.getClass() != obj.getClass())
        return false;

      return value == ((M3PToken)obj).value;
    }

    @Override
    public int hashCode() {
      return (int)(value^(value>>>32));
    }
  }

  public static class OPPToken extends Token {
    private final ByteBuffer value;


    OPPToken(ByteBuffer value) {
      this.value = value;
    }

    @Override
    public int compareTo(Object other) {
      if ( other != null && other instanceof OPPToken )
        return value.compareTo(((OPPToken)other).value);
      throw new IllegalArgumentException("Attempt to compare on null or wrong token type");
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj)
        return true;
      if (obj == null || this.getClass() != obj.getClass())
        return false;

      return value.equals(((OPPToken)obj).value);
    }

    @Override
    public int hashCode() {
      return value.hashCode();
    }
  }


}
