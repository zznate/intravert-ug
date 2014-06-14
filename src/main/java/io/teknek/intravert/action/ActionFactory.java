package io.teknek.intravert.action;

import java.util.HashMap;
import java.util.Map;

import io.teknek.intravert.action.impl.CreateColumnFamilyAction;
import io.teknek.intravert.action.impl.CreateFilterAction;
import io.teknek.intravert.action.impl.CreateKeyspaceAction;
import io.teknek.intravert.action.impl.GetKeyspaceAction;
import io.teknek.intravert.action.impl.SaveSessionAction;
import io.teknek.intravert.action.impl.LoadSessionAction;
import io.teknek.intravert.action.impl.SetKeyspaceAction;
import io.teknek.intravert.action.impl.UpsertAction;

public class ActionFactory {
  
  public static final String CREATE_SESSION = "createsession";
  public static final String LOAD_SESSION = "loadsession";
  public static final String SET_KEYSPACE = "setkeyspace";
  public static final String GET_KEYSPACE = "getkeyspace";
  public static final String CREATE_FILTER = "createfilter";
  public static final String UPSERT = "upsert";
  public static final String CREATE_KEYSPACE = "createkeyspace";
  public static final String CREATE_COLUMN_FAMILY ="createcolumnfamily";
  private Map<String,Action> actions;
  
  public ActionFactory(){
    actions = new HashMap<String,Action>();
    actions.put(CREATE_SESSION, new SaveSessionAction());
    actions.put(LOAD_SESSION, new LoadSessionAction());
    actions.put(SET_KEYSPACE, new SetKeyspaceAction()); 
    actions.put(GET_KEYSPACE, new GetKeyspaceAction());
    actions.put(CREATE_FILTER, new CreateFilterAction());
    actions.put(UPSERT, new UpsertAction());
    actions.put(CREATE_KEYSPACE, new CreateKeyspaceAction());
    actions.put(CREATE_COLUMN_FAMILY, new CreateColumnFamilyAction());
  }
  
  public Action findAction(String operation){
    Action a = actions.get(operation);
    if (a == null)
      throw new IllegalArgumentException("Do not know what to do with " + operation);
    return a;
  }
}
