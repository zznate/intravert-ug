package io.teknek.intravert.action.filter;

import java.util.Map;

import groovy.lang.Closure;

public class FilterFactory {
  
  public static Filter createFilter(final Object object){
    if (object instanceof Filter){
      return (Filter) object;
    } else if (object instanceof Closure){
      return new Filter(){
        public Map filter(Map m) {
          return ((Closure<Map>)object).call(m);
        }
      };
    } else {
      throw new RuntimeException("Do not know what to do with "+ object);
    }
  }

}