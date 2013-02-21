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
package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;

import org.apache.cassandra.db.marshal.AbstractType;
import org.apache.cassandra.db.marshal.TypeParser;
import org.apache.cassandra.exceptions.ConfigurationException;
import org.apache.cassandra.exceptions.SyntaxException;
import org.apache.cassandra.utils.ByteBufferUtil;

public class TypeHelper {
  public static Object getTypedIfPossible(IntraState state, String type, ByteBuffer bb, IntraOp op){
	  
    IntraMetaData imd = new IntraMetaData(IntraService.determineKs(null ,op, state),IntraService.determineCf(null, op, state),type);
    String s = state.meta.get(imd);
      if (s == null) {
          return bb;
      }
      return compose(bb, s);
  }

    public static Object getTyped(String type, ByteBuffer bb){
    if (type.equals("UTF-8")){
      try {
        return ByteBufferUtil.string(bb);
      } catch (Exception ex){ throw new RuntimeException(ex); }
    } else if (type.equals("int32")) {
      return ByteBufferUtil.toInt(bb);
    } else {
      return bb;
    }
  }

    public static Object getCqlTyped(String type, ByteBuffer bb) {
        if (bb == null) {
            return null;
        }
        return compose(bb, type);
    }

    private static Object compose(ByteBuffer bb, String s) {
        try {
            AbstractType<?> abstractType = TypeParser.parse(s);
            return abstractType.compose(bb);
        } catch (SyntaxException | ConfigurationException e) {
            throw new RuntimeException("Failed to parse type [" + s + "]", e);
        }
    }
}
