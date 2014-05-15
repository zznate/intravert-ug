package io.teknek.intravert.service;

public class Session {
  
  private String currentKeyspace;
  
  public String getCurrentKeyspace() {
    return currentKeyspace;
  }

  public void setCurrentKeyspace(String currentKeyspace) {
    this.currentKeyspace = currentKeyspace;
  }

}
