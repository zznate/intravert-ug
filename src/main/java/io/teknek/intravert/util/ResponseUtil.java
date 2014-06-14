package io.teknek.intravert.util;

import io.teknek.intravert.model.Constants;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ResponseUtil {

  public static List getDefaultHappy(){
    Map m = new HashMap();
    m.put(Constants.RESULT, Constants.OK);
    return Arrays.asList(m);
  }
}
