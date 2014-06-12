package io.teknek.intravert.service;

import io.teknek.intravert.action.filter.Filter;

import java.util.HashMap;
import java.util.Map;

public class ApplicationContext {

 private Map<String,Filter> filters;
  
  public ApplicationContext(){
    filters = new HashMap<>();
  }

  public void putFilter(String name, Filter f){
    filters.put(name, f);
  }
  
  public Filter getFilter(String name){
    Filter f = filters.get(name);
    //todo get from column family
    if (f == null){
      throw new RuntimeException ("filter not found");
    }
    return f;
  }
}
