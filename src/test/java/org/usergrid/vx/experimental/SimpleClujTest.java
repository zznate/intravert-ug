package org.usergrid.vx.experimental;

import java.io.IOException;
import java.io.StringReader;
import java.util.HashMap;
import java.util.Map;

import junit.framework.Assert;

import org.junit.Test;

import clojure.lang.IPersistentMap;
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
    Assert.assertEquals("Hi there", result);
  }
  
  @Test
  public void bTest() throws ClassNotFoundException, IOException {
    //RT.init();
    RT.load("clojure/core");
    //String str = "(ns user) (defn fil [a] ( map? a))";
    //String str = "(ns user) (defn fil [a] (  a \"value\" ))";
    //String str = "(ns user) (defn fil [a] (= (a \"value\") \"y\" ))";
    //String str =   "(ns user) (defn fil [a] (if (= (a \"value\"), \"y\" ) (a) () )";
    /*
     * (defn abs [x]
  (if (< x 0)
    (- x)
    x))
     */
    String str = "(ns user) (defn fil [a] (if (= (a \"value\") \"y\" ) a           )           )";
    Compiler.load(new StringReader(str));  
    Var foo = RT.var("user", "fil");
    Map m = new HashMap();
    m.put("name", "x");
    m.put("value", "y");
    
    IPersistentMap m2 = clojure.lang.RT.map( m );
    m2 = m2.assoc("name", "x");
    m2 = m2.assoc("value", "y");
    Object result = foo.invoke( m2 );
    Assert.assertEquals(m, result);
  }
  
  
  
  
}
