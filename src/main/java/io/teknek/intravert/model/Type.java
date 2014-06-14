package io.teknek.intravert.model;

public class Type {

  private String theClass;
  private Object value;
  
  public Type(Object o){
    setValue(o);
    setTheClass(o.getClass().getSimpleName());
  }
  
  public Type(String theClass, Object o){
    setTheClass(theClass);
    setValue(o);
  }
  
  public String getTheClass() {
    return theClass;
  }
  
  public void setTheClass(String theClass) {
    this.theClass = theClass;
  }
  
  public Object getValue() {
    return value;
  }
  
  public void setValue(Object theValue) {
    this.value = theValue;
  }
  
}
