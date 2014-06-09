package io.teknek.intravert.service;

import io.teknek.intravert.action.filter.Filter;

import java.util.HashMap;
import java.util.Map;

public class Session {
  
  private String currentKeyspace;
  private Map<String,Filter> filters;
  
  public Session(){
    filters = new HashMap<>();
  }
  
  public String getCurrentKeyspace() {
    return currentKeyspace;
  }

  public void setCurrentKeyspace(String currentKeyspace) {
    this.currentKeyspace = currentKeyspace;
  }

  public void putFilter(String name, Filter f){
    filters.put(name, f);
  }
  
  public Filter getFilter(String name){
    Filter f = filters.get(name);
    if (f == null){
      throw new RuntimeException ("filter not found");
    }
    return f;
  }
}
