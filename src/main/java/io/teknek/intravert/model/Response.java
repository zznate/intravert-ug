package io.teknek.intravert.model;

import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

public class Response {
  private String exceptionMessage;
  private String exceptionId;
  private LinkedHashMap<String,Object> results;
  private LinkedHashMap<String,Object> metaData;
  
  public Response(){
    results = new LinkedHashMap<String,Object>();
    metaData = new LinkedHashMap<String,Object>();
  }

  public String getExceptionMessage() {
    return exceptionMessage;
  }

  public void setExceptionMessage(String exceptionMessage) {
    this.exceptionMessage = exceptionMessage;
  }

  public String getExceptionId() {
    return exceptionId;
  }

  public void setExceptionId(String exceptionId) {
    this.exceptionId = exceptionId;
  }

  public Map<String, Object> getResults() {
    return results;
  }

  public void setResults(LinkedHashMap<String, Object> results) {
    this.results = results;
  }

  public Map<String, Object> getMetaData() {
    return metaData;
  }

  public void setMetaData(LinkedHashMap<String, Object> metaData) {
    this.metaData = metaData;
  }
  
}
