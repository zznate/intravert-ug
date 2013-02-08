/* 
 *   Copyright 2013 Nate McCall and Edward Capriolo
 *
 *   Licensed under the Apache License, Version 2.0 (the "License");
 *   you may not use this file except in compliance with the License.
 *   You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *   Unless required by applicable law or agreed to in writing, software
 *   distributed under the License is distributed on an "AS IS" BASIS,
 *   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *   See the License for the specific language governing permissions and
 *   limitations under the License.
*/
package org.usergrid.vx.client.pooling;

import java.math.BigInteger;
import java.nio.ByteBuffer;

/**
 * Originally derived from the very well done CQL Java Driver
 * by the good folks at DataStax: https://github.com/datastax/java-driver
 *
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
