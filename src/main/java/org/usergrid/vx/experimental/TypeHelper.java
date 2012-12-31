package org.usergrid.vx.experimental;

import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.util.List;

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
    } else if (s.equals("int32")) {
      return ByteBufferUtil.toInt(bb);
    } else if (s.startsWith("CompositeType")){
      int start = s.indexOf("(");
      int end = s.indexOf(")");
      String list = s.substring(start+1,end);
      System.out.println("list is" + list);
      
      String [] parts = list.split(",");
      Object [] results = new Object[parts.length] ;
      System.out.println("parts " + parts.length);
      byte[] by = new byte[bb.remaining()];
      bb.get(by);
      List<byte[]> comp = CompositeTool.readComposite(by);
      System.out.println("results size "+results.length);
      System.out.println("comp size"+ comp.size());
      for (int i=0;i<parts.length;i++){
        results[i]= getTyped(parts[i], ByteBuffer.wrap(comp.get(i)) ); 
      }
      return results;
    } else {
      throw new RuntimeException("Do not know what to do with "+s);
    }
  }
  private static Object getTyped(String type, ByteBuffer bb){
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
}
