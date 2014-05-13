package io.teknek.intravert.model;

import java.util.List;

public class Request {
  private List<Operation> operations;
  
  public Request(){
    
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public void setOperations(List<Operation> operations) {
    this.operations = operations;
  }
  
}
