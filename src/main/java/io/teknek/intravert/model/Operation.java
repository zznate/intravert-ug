package io.teknek.intravert.model;


import java.util.Map;

public class Operation {
  private String type;
  private String id;
  private Map<String,Object> arguments;
  
  public Operation(){
    
  }

  public String getType() {
    return type;
  }

  public void setType(String type) {
    this.type = type;
  }

  public Map<String, Object> getArguments() {
    return arguments;
  }

  public void setArguments(Map<String, Object> arguments) {
    this.arguments = arguments;
  }

  public String getId() {
    return id;
  }

  public void setId(String id) {
    this.id = id;
  }
  
}
