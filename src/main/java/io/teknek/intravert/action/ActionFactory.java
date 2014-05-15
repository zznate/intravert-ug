package io.teknek.intravert.action;

import io.teknek.intravert.action.impl.SaveSessionAction;
import io.teknek.intravert.action.impl.LoadSessionAction;
import io.teknek.intravert.action.impl.SetKeyspaceAction;

public class ActionFactory {
  
  public Action findAction(String operation){
    if (operation.equalsIgnoreCase("createsession")){
      return new SaveSessionAction(); 
    }
    if (operation.equalsIgnoreCase("loadsession")){
      return new LoadSessionAction(); 
    }
    if (operation.equalsIgnoreCase("setkeyspace")){
      return new SetKeyspaceAction();
    }
    throw new IllegalArgumentException("Do not know what to do with " + operation);
  }
}
