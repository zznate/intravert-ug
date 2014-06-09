package io.teknek.intravert.model;

import java.util.HashMap;
import java.util.Map;

public class Response {
  private String exceptionMessage;
  private String exceptionId;
  private Map<String,Object> results;
  private Map<String,Object> metaData;
  
  public Response(){
    results = new HashMap<String,Object>();
    metaData = new HashMap<String,Object>();
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

  public void setResults(Map<String, Object> results) {
    this.results = results;
  }

  public Map<String, Object> getMetaData() {
    return metaData;
  }

  public void setMetaData(Map<String, Object> metaData) {
    this.metaData = metaData;
  }
  
}
