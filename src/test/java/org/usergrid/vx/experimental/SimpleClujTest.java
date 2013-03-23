package org.usergrid.vx.experimental;

import java.io.IOException;
import java.io.StringReader;

import org.junit.Test;

import clojure.lang.RT;
import clojure.lang.Var;
import clojure.lang.Compiler;

public class SimpleClujTest {

  @Test
  public void aTest() throws ClassNotFoundException, IOException {
    //RT.init();
    RT.load("clojure/core");
    String str = "(ns user) (defn foo [a b]   (str a \" \" b))";
    Compiler.load(new StringReader(str));  
    Var foo = RT.var("user", "foo");
    Object result = foo.invoke("Hi", "there");
    System.out.println(result);
  }
}
