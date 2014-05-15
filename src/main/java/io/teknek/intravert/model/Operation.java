package io.teknek.intravert.model;


import java.util.HashMap;
import java.util.Map;

public class Operation {
  private String type;
  private String id;
  private Map<String,Object> arguments;
  
  public Operation(){
    arguments = new HashMap<String,Object>();
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
  
  public Operation withId(String id){
    setId(id);
    return this;
  }
  
  public Operation withType(String type){
    setType(type);
    return this;
  }
  
  public Operation withArguments(Map<String,Object> arguments){
    setArguments(arguments);
    return this;
  }
  
  public Map<String,Object> withArgument(String name, Object value){
    arguments.put(name, value);
    return arguments;
  }
}
