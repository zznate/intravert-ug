package io.teknek.intravert.model;

import java.util.ArrayList;
import java.util.List;

public class Request {
  private List<Operation> operations;
  
  public Request(){
    operations = new ArrayList<Operation>();
  }

  public List<Operation> getOperations() {
    return operations;
  }

  public void setOperations(List<Operation> operations) {
    this.operations = operations;
  }
  
}
