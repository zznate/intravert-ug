package io.teknek.intravert.service;

public class Session {
  private Long requestId;
  private String currentKeyspace;
  
  public Long getRequestId() {
    return requestId;
  }

  public void setRequestId(Long requestId) {
    this.requestId = requestId;
  }

  public String getCurrentKeyspace() {
    return currentKeyspace;
  }

  public void setCurrentKeyspace(String currentKeyspace) {
    this.currentKeyspace = currentKeyspace;
  }

}
