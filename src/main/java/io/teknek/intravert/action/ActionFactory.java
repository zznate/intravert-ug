package io.teknek.intravert.action;

import java.util.HashMap;
import java.util.Map;

import io.teknek.intravert.action.impl.GetKeyspaceAction;
import io.teknek.intravert.action.impl.SaveSessionAction;
import io.teknek.intravert.action.impl.LoadSessionAction;
import io.teknek.intravert.action.impl.SetKeyspaceAction;

public class ActionFactory {
  
  public static final String CREATE_SESSION = "createsession";
  public static final String LOAD_SESSION = "loadsession";
  public static final String SET_KEYSPACE = "setkeyspace";
  public static final String GET_KEYSPACE = "getkeyspace";
  private Map<String,Action> actions;
  
  public ActionFactory(){
    actions = new HashMap<String,Action>();
    actions.put(CREATE_SESSION, new SaveSessionAction());
    actions.put(LOAD_SESSION, new LoadSessionAction());
    actions.put(SET_KEYSPACE, new SetKeyspaceAction());
    actions.put(GET_KEYSPACE, new GetKeyspaceAction());
  }
  
  public Action findAction(String operation){
    Action a = actions.get(operation);
    if (a == null)
      throw new IllegalArgumentException("Do not know what to do with " + operation);
    return a;
  }
}
