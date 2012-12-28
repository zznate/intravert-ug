package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;

import org.apache.cassandra.utils.ByteBufferUtil;

public class TypeHelper {
  public static Object getTypedIfPossible(IntraState state, String type, ByteBuffer bb){
    IntraMetaData imd = new IntraMetaData(state.currentKeyspace,state.currentColumnFamily,type);
    String s = state.meta.get(imd);
    
    if (s == null){
      return bb;
    } else if (s.equals("UTF-8")){
      try {
        return ByteBufferUtil.string(bb);
      } catch (Exception ex){ throw new RuntimeException(ex); }
    } else {
      throw new RuntimeException("Do not know what to do");
    }
  }
}
