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
