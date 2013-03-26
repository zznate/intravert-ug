package org.usergrid.vx.experimental.filter.clojure;

import java.io.IOException;
import java.io.StringReader;

import org.usergrid.vx.experimental.filter.Filter;
import org.usergrid.vx.experimental.filter.FilterFactory;

import clojure.lang.Compiler;
import clojure.lang.RT;
import clojure.lang.Var;

public class ClojureFilterFactory implements FilterFactory {

  @Override
  public Filter createFilter(String script) {
    try {
      RT.load("clojure/core");
    } catch (ClassNotFoundException | IOException e) {
      e.printStackTrace();
    }
    Object result =  Compiler.load(new StringReader(script));
    if (result instanceof Filter) {
      return (Filter) result;
    } else if (result instanceof Var){
      Var r = (Var) result;
      return new ClojureFilter(r);
    } else {
      throw new IllegalArgumentException(script );
    }
  }

}
