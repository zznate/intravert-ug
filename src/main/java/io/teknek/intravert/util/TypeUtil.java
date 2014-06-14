package io.teknek.intravert.util;

import io.teknek.intravert.model.Type;

import java.util.Map;

import org.codehaus.jackson.map.ObjectMapper;

public class TypeUtil {
  private static ObjectMapper om = new ObjectMapper();
  public static <T> T convert(Object in){
    if (in instanceof Map){
      Type t = om.convertValue(in, Type.class);
      if ("blob".equalsIgnoreCase(t.getTheClass())){
        return (T) ((String) t.getValue()).getBytes();
      } else {
        return (T) t.getValue();
      }
    }
    return (T) in;
  }
}
