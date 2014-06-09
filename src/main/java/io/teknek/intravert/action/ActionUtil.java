package io.teknek.intravert.action;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ActionUtil {

  public static List<Map<String, Object>> simpleResult (String key, Object value){
    Map<String,Object> m = new HashMap<>();
    m.put(key, value);
    return  Arrays.asList(m);
  }
}
