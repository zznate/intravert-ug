package org.usergrid.vx.experimental.filter.clojure;

import java.util.Map;
import org.usergrid.vx.experimental.filter.Filter;
import clojure.lang.IPersistentMap;
import clojure.lang.Var;

public class ClojureFilter implements Filter{

  private Var v;
  
  public ClojureFilter(Var foo){
    v=foo;
  }
  
  @Override
  public Map filter(Map row) {
    IPersistentMap m2 = clojure.lang.RT.map(  );
    for (Object pair : row.entrySet()){
      Map.Entry e = (Map.Entry) pair;
      m2= m2.assoc(e.getKey(), e.getValue());
    }
    return (Map) v.invoke(m2);
  }

}